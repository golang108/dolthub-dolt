// Copyright 2021 Dolthub, Inc.
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

package sqle

import (
	"context"
	"errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/types"
)

type ReadReplicaDatabase struct {
	Database
	headRef        ref.DoltRef
	remoteTrackRef ref.DoltRef
	remote         env.Remote
	srcDB          *doltdb.DoltDB
	localBranches  map[string]ref.DoltRef
	tmpDir         string
}

var _ SqlDatabase = ReadReplicaDatabase{}
var _ sql.VersionedDatabase = ReadReplicaDatabase{}
var _ sql.TableDropper = ReadReplicaDatabase{}
var _ sql.TableCreator = ReadReplicaDatabase{}
var _ sql.TemporaryTableCreator = ReadReplicaDatabase{}
var _ sql.TableRenamer = ReadReplicaDatabase{}
var _ sql.TriggerDatabase = &ReadReplicaDatabase{}
var _ sql.StoredProcedureDatabase = ReadReplicaDatabase{}
var _ sql.TransactionDatabase = ReadReplicaDatabase{}

var EmptyReadReplica = ReadReplicaDatabase{}

var ErrFailedToCastToReplicaDb = errors.New("failed to cast to ReadReplicaDatabase")
var ErrCannotCreateReplicaRevisionDbForCommit = errors.New("cannot create replica revision db for commit")

func NewReadReplicaDatabase(ctx context.Context, db Database, remoteName string, rsr env.RepoStateReader, tmpDir string, meta *doltdb.WorkingSetMeta) (ReadReplicaDatabase, error) {
	remotes, err := rsr.GetRemotes()
	if err != nil {
		return EmptyReadReplica, err
	}

	remote, ok := remotes[remoteName]
	if !ok {
		return EmptyReadReplica, env.ErrRemoteNotFound
	}

	srcDB, err := remote.GetRemoteDB(ctx, types.Format_Default)
	if err != nil {
		return EmptyReadReplica, err
	}

	headRef := rsr.CWBHeadRef()
	rtRef, err := remoteTrackingRef(db.rsr, headRef, remoteName)
	if err != nil {
		return EmptyReadReplica, err
	}

	branches, err := db.ddb.GetBranches(ctx)
	localBranches := make(map[string]ref.DoltRef, len(branches))
	for _, br := range branches {
		localBranches[br.String()] = br
	}

	return ReadReplicaDatabase{
		Database:       db,
		headRef:        headRef,
		remoteTrackRef: rtRef,
		remote:         remote,
		tmpDir:         tmpDir,
		srcDB:          srcDB,
		localBranches:  localBranches,
	}, nil
}

func (rrd ReadReplicaDatabase) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	err := rrd.PullFromReplica(ctx)
	if err != nil {
		return nil, err
	}
	return rrd.Database.StartTransaction(ctx, tCharacteristic)
}

func (rrd ReadReplicaDatabase) SetHeadRef(head ref.DoltRef) (ReadReplicaDatabase, error) {
	rtRef, err := remoteTrackingRef(rrd.rsr, head, rrd.remote.Name)
	if err != nil {
		return rrd, err
	}
	rrd.remoteTrackRef = rtRef
	rrd.headRef = head
	return rrd, nil
}

func (rrd ReadReplicaDatabase) PullFromReplica(ctx context.Context) error {
	if _, val, ok := sql.SystemVariables.GetGlobal(doltdb.ReplicateHeadsStrategy); ok {
		switch val {
		case doltdb.ReplicateHeads_MANY:
			err := fetchBranches(ctx, rrd)
			if err != nil {
				return err
			}
			return fetchHead(ctx, rrd)
		case doltdb.ReplicateHeads_ONE:
			return fetchHead(ctx, rrd)
		default:
			return fetchHead(ctx, rrd)
		}
	} else {
		return sql.ErrUnknownSystemVariable.New(doltdb.ReplicateHeadsStrategy)
	}
}

func remoteTrackingRef(rsr env.RepoStateReader, headRef ref.DoltRef, remote string) (ref.DoltRef, error) {
	refSpecs, err := env.GetRefSpecs(rsr, remote)
	if err != nil {
		return nil, err
	}

	var remoteTrackRef ref.DoltRef
	var foundRef bool
	for _, refSpec := range refSpecs {
		trackRef := refSpec.DestRef(headRef)
		if trackRef != nil {
			remoteTrackRef = trackRef
			foundRef = true
			break
		}
	}
	if !foundRef {
		return nil, env.ErrInvalidRefSpecRemote
	}
	return remoteTrackRef, nil
}

func fetchHead(ctx context.Context, rrd ReadReplicaDatabase) error {
	ddb := rrd.Database.DbData().Ddb
	err := rrd.srcDB.Rebase(ctx)
	if err != nil {
		return err
	}
	srcDBCommit, err := actions.FetchRemoteBranch(ctx, rrd.tmpDir, rrd.remote, rrd.srcDB, ddb, rrd.headRef, nil, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	if err != nil {
		return err
	}

	err = ddb.FastForward(ctx, rrd.remoteTrackRef, srcDBCommit)
	if err != nil {
		return err
	}

	err = ddb.FastForward(ctx, rrd.headRef, srcDBCommit)
	if err != nil {
		return err
	}

	wsRef, err := ref.WorkingSetRefForHead(rrd.headRef)
	if err != nil {
		return err
	}

	ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
	if err != nil {
		return err
	}

	commitRoot, err := srcDBCommit.GetRootValue()
	if err != nil {
		return err
	}

	ws = ws.WithWorkingRoot(commitRoot).WithStagedRoot(commitRoot)

	h, err := ws.HashOf()
	if err != nil {
		return err
	}
	rrd.ddb.UpdateWorkingSet(ctx, ws.Ref(), ws, h, doltdb.TodoWorkingSetMeta())

	return nil
}

func fetchBranches(ctx context.Context, rrd ReadReplicaDatabase) error {
	err := rrd.srcDB.Rebase(ctx)
	if err != nil {
		return err
	}

	refs, err := rrd.srcDB.GetBranches(ctx)
	if err != nil {
		return err
	}

	args := make([]string, 0, len(refs))
	for _, r := range refs {
		args = append(args, r.GetPath())
	}
	refSpecs, err := env.ParseRSFromArgs(rrd.remote.Name, args)

	updateMode := ref.FastForwardOnly
	err = actions.FetchRefSpecs(ctx, rrd.DbData(), refSpecs, rrd.remote, updateMode, actions.NoopRunProgFuncs, actions.NoopStopProgFuncs)
	if err != nil {
		return err
	}

	return nil
}
