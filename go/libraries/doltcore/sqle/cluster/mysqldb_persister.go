// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/sirupsen/logrus"

	replicationapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/replicationapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

type MySQLDbPersister interface {
	mysql_db.MySQLDbPersistence
	LoadData(context.Context) ([]byte, error)
}

type replicatingMySQLDbPersister struct {
	base MySQLDbPersister

	current  []byte
	version  uint32
	replicas []*mysqlDbReplica
	role     Role
	started  bool

	mu sync.Mutex
}

type mysqlDbReplica struct {
	client *replicationServiceClient
	lgr    *logrus.Entry
	wg     sync.WaitGroup

	callReqCh  chan MakeUpdateUsersAndGrantsCallRequest
	callRespCh chan MakeUpdateUsersAndGrantsCallResponse

	updateReqCh          chan UpdateMySQLDbRequest
	setFastFailWaitReqCh chan SetFastFailWaitRequest
	setRoleReqCh         chan SetRoleRequest
	waitNotifyReqCh      chan SetWaitNotifyRequest
	doneCh               chan struct{}
}

type UpdateMySQLDbRequest struct {
	Contents []byte
	Version  uint32
	RespCh   chan UpdateMySQLDbResponse
}

type UpdateMySQLDbResponse struct {
	WaitF func(context.Context) error
}

type SetFastFailWaitRequest struct {
	Value bool
}

type IsCaughtUpRequest struct {
	RespCh chan bool
}

type MakeUpdateUsersAndGrantsCallRequest struct {
	Contents []byte
	Version  uint32
}

type MakeUpdateUsersAndGrantsCallResponse struct {
	Err     error
	Version uint32
}

type SetWaitNotifyRequest struct {
	NotifyF func(caughtUp bool)
	RespCh  chan bool
}

type SetRoleRequest struct {
	Role Role
}

func (r *mysqlDbReplica) UpdateMySQLDb(ctx context.Context, contents []byte, version uint32) func(context.Context) error {
	respCh := make(chan UpdateMySQLDbResponse, 1)
	req := UpdateMySQLDbRequest{
		Contents: contents,
		Version:  version,
		RespCh:   respCh,
	}
	r.updateReqCh <- req
	resp := <-respCh
	return resp.WaitF
}

func (r *mysqlDbReplica) Run(role Role, version uint32, contents []byte) {
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		r.runReqRespLoop(role, version, contents)
	}()
	go func() {
		defer r.wg.Done()
		r.runCallLoop()
	}()
	r.wg.Wait()
}

func (r *mysqlDbReplica) GracefulStop() {
	close(r.doneCh)
	r.wg.Wait()
}

func (r *mysqlDbReplica) runReqRespLoop(role Role, version uint32, contents []byte) {
	var attempt *Attempt
	var replicatedVersion uint32
	var waitNotify func(caughtUp bool)
	var progressNotifier ProgressNotifier
	var fastFailReplicationWait bool
	var nextAttempt time.Time

	timer := time.NewTimer(0)
	timerActive := true

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = time.Second
	backoff.MaxInterval = time.Minute
	backoff.MaxElapsedTime = 0

	for {
		sleepDuration := nextAttempt.Sub(time.Now())
		needsToSleep := sleepDuration > 0

		hasSomethingToReplicate := role == RolePrimary && version != 0 && replicatedVersion != version

		wantsToReplicate := hasSomethingToReplicate && !needsToSleep

		isCaughtUp := !hasSomethingToReplicate

		if wantsToReplicate && len(contents) == 0 {
			r.lgr.Debugf("mysqlDbReplica[%s]: not replicating empty users and grants at version %d.", r.client.remote, version)
			replicatedVersion = version

			continue
		}

		callReq := MakeUpdateUsersAndGrantsCallRequest{
			Contents: contents,
			Version:  version,
		}
		var callReqCh chan MakeUpdateUsersAndGrantsCallRequest
		if wantsToReplicate {
			callReqCh = r.callReqCh
		} else {
			if waitNotify != nil {
				waitNotify(isCaughtUp)
			}
			r.lgr.Infof("mysqlDbReplica waiting...")
			if isCaughtUp {
				attempt := progressNotifier.BeginAttempt()
				progressNotifier.RecordSuccess(attempt)
			}
		}

		var sleepCh <-chan time.Time
		if needsToSleep {
			if timerActive && !timer.Stop() {
				<-timer.C
			}
			timerActive = true
			timer.Reset(sleepDuration)
			sleepCh = timer.C
		}

		select {
		case req := <-r.updateReqCh:
			r.lgr.Infof("mysqlDbReplica got new contents at version %d", req.Version)
			contents = req.Contents
			version = req.Version
			nextAttempt = time.Time{}
			backoff.Reset()
			var resp UpdateMySQLDbResponse
			if fastFailReplicationWait {
				remote := r.client.remote
				resp.WaitF = func(ctx context.Context) error {
					return fmt.Errorf("circuit breaker for replication to %s/mysql is open. this update to users and grants did not necessarily replicate successfully.", remote)
				}
			} else {
				w := progressNotifier.Wait()

				resp.WaitF = func(ctx context.Context) error {
					err := w(ctx)
					if err != nil && errors.Is(err, doltdb.ErrReplicationWaitFailed) {
						r.setFastFailReplicationWait(true)
					}
					return err
				}
			}
			req.RespCh <- resp
		case callReqCh <- callReq:
			attempt = progressNotifier.BeginAttempt()
		case callResp := <-r.callRespCh:
			if callResp.Err != nil {
				progressNotifier.RecordFailure(attempt)
				r.lgr.Warnf("mysqlDbReplica[%s]: error replicating users and grants. backing off. %v", r.client.remote, callResp.Err)
				nextAttempt = time.Now().Add(backoff.NextBackOff())
			} else {
				progressNotifier.RecordSuccess(attempt)
				fastFailReplicationWait = false
				backoff.Reset()
				r.lgr.Debugf("mysqlDbReplica[%s]: sucessfully replicated users and grants at version %d.", r.client.remote, callResp.Version)
				replicatedVersion = callResp.Version
			}
			attempt = nil
		case req := <-r.setFastFailWaitReqCh:
			fastFailReplicationWait = req.Value
		case req := <-r.waitNotifyReqCh:
			if req.NotifyF != nil && waitNotify != nil {
				req.RespCh <- false
			} else {
				waitNotify = req.NotifyF
				if waitNotify != nil {
					waitNotify(isCaughtUp)
				}
				req.RespCh <- true
			}
		case req := <-r.setRoleReqCh:
			role = req.Role
			nextAttempt = time.Time{}
			backoff.Reset()
		case <-sleepCh:
			timerActive = false
		case <-r.doneCh:
			return
		}
	}

}

func (r *mysqlDbReplica) setFastFailReplicationWait(v bool) {
	r.setFastFailWaitReqCh <- SetFastFailWaitRequest{
		Value: v,
	}
}

func (r *mysqlDbReplica) setWaitNotify(callback func(bool)) bool {
	req := SetWaitNotifyRequest{
		NotifyF: callback,
		RespCh:  make(chan bool, 1),
	}
	r.waitNotifyReqCh <- req
	return <-req.RespCh
}

func (r *mysqlDbReplica) setRole(role Role) {
	r.setRoleReqCh <- SetRoleRequest{role}
}

func (r *mysqlDbReplica) runCallLoop() {
	for {
		var req MakeUpdateUsersAndGrantsCallRequest
		select {
		case <-r.doneCh:
			return
		case req = <-r.callReqCh:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := r.client.client.UpdateUsersAndGrants(ctx, &replicationapi.UpdateUsersAndGrantsRequest{
			SerializedContents: req.Contents,
		})
		cancel()

		resp := MakeUpdateUsersAndGrantsCallResponse{
			Err:     err,
			Version: req.Version,
		}

		select {
		case <-r.doneCh:
			return
		case r.callRespCh <- resp:
		}
	}
}

func (p *replicatingMySQLDbPersister) setRole(role Role) {
	p.mu.Lock()
	p.role = role
	// If we are transitioning to primary and we are already initialized,
	// then we reload data so that we have the most recent persisted users
	// and grants to replicate.
	needsLoad := p.version != 0 && role == RolePrimary
	started := p.started
	p.mu.Unlock()
	if started {
		for _, r := range p.replicas {
			r.setRole(role)
		}
	}
	if needsLoad {
		p.LoadData(context.Background())
	}
}

func (p *replicatingMySQLDbPersister) Run() {
	p.mu.Lock()
	role := p.role
	version := p.version
	contents := p.current
	started := p.started
	p.started = true
	p.mu.Unlock()
	if started {
		panic("cannot run replicatingMySQLDbPersister twice")
	}
	var wg sync.WaitGroup
	for _, r := range p.replicas {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Run(role, version, contents)
		}()
	}
	wg.Wait()
}

func (p *replicatingMySQLDbPersister) GracefulStop() {
	var wg sync.WaitGroup
	wg.Add(len(p.replicas))
	for _, r := range p.replicas {
		go func() {
			defer wg.Done()
			r.GracefulStop()
		}()
	}
	wg.Wait()
}

func (p *replicatingMySQLDbPersister) Persist(ctx *sql.Context, data []byte) error {
	p.mu.Lock()
	err := p.base.Persist(ctx, data)
	if err == nil {
		p.current = data
		p.version += 1
		var rsc doltdb.ReplicationStatusController
		rsc.Wait = make([]func(context.Context) error, len(p.replicas))
		rsc.NotifyWaitFailed = make([]func(), len(p.replicas))
		for i, r := range p.replicas {
			rsc.Wait[i] = r.UpdateMySQLDb(ctx, p.current, p.version)
			rsc.NotifyWaitFailed[i] = func() {}
		}
		p.mu.Unlock()
		dsess.WaitForReplicationController(ctx, rsc)
	} else {
		p.mu.Unlock()
	}
	return err
}

func (p *replicatingMySQLDbPersister) LoadData(ctx context.Context) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ret, err := p.base.LoadData(ctx)
	if err == nil {
		p.current = ret
		p.version += 1
		if p.started {
			for _, r := range p.replicas {
				r.UpdateMySQLDb(ctx, p.current, p.version)
			}
		}
	}
	return ret, err
}

func (p *replicatingMySQLDbPersister) waitForReplication(timeout time.Duration) ([]graceTransitionResult, error) {
	p.mu.Lock()
	replicas := make([]*mysqlDbReplica, len(p.replicas))
	copy(replicas, p.replicas)
	res := make([]graceTransitionResult, len(replicas))
	for i := range replicas {
		res[i].database = "mysql"
		res[i].remote = replicas[i].client.remote
		res[i].remoteUrl = replicas[i].client.httpUrl()
	}
	var wg sync.WaitGroup
	wg.Add(len(replicas))
	for li, r := range replicas {
		i := li
		ok := r.setWaitNotify(func(caughtUp bool) {
			if !res[i].caughtUp && caughtUp {
				res[i].caughtUp = true
				wg.Done()
			}
		})
		if !ok {
			for j := li - 1; j >= 0; j-- {
				replicas[j].setWaitNotify(nil)
			}
			return nil, errors.New("cluster: mysqldb replication: could not wait for replication. Concurrent waiters conflicted with each other.")
		}
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range replicas {
		r.setWaitNotify(nil)
	}

	// Make certain we don't leak the wg.Wait goroutine in the failure case.
	// At this point, none of the callbacks will ever be called again and
	// ch.setWaitNotify grabs a lock and so establishes the happens before.
	for _, b := range res {
		if !b.caughtUp {
			wg.Done()
		}
	}
	<-done

	return res, nil
}
