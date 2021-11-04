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

package doltdb

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"

	"github.com/dolthub/dolt/go/store/datas"
)

const (
	ReplicateToRemoteKey = "dolt_replicate_to_remote"
	DoltReadReplicaKey   = "dolt_read_replica_remote"
	ReplicateHeadsMode   = "dolt_replicate_heads_mode"
)

const (
	ReplicateHeads_ONE  = "one"
	ReplicateHeads_MANY = "many"
)

type ReplicateHook struct {
	destDB datas.Database
	tmpDir string
	outf   io.Writer
}

// NewReplicateHook creates a ReplicateHook, parameterizaed by the backup database
// and a local tempfile for pushing
func NewReplicateHook(destDB *DoltDB, tmpDir string) *ReplicateHook {
	return &ReplicateHook{destDB: destDB.db, tmpDir: tmpDir}
}

// Execute implements datas.CommitHook, replicates head updates to the destDb field
func (rh *ReplicateHook) Execute(ctx context.Context, ds datas.Dataset, db datas.Database) error {
	return replicate(ctx, rh.destDB, db, rh.tmpDir, ds)
}

// HandleError implements datas.CommitHook
func (rh *ReplicateHook) HandleError(ctx context.Context, err error) error {
	if rh.outf != nil {
		rh.outf.Write([]byte(err.Error()))
	}
	return nil
}

// SetLogger implements datas.CommitHook
func (rh *ReplicateHook) SetLogger(ctx context.Context, wr io.Writer) error {
	rh.outf = wr
	return nil
}

// replicate pushes a dataset from srcDB to destDB and force sets the destDB ref to the new dataset value
func replicate(ctx context.Context, destDB, srcDB datas.Database, tempTableDir string, ds datas.Dataset) error {
	//refsToPush := make([]types.Ref, 0)
	stRef, ok, err := ds.MaybeHeadRef()
	if err != nil {
		return err
	}
	if !ok {
		// No head ref, return
		return nil
	}

	rf, err := ref.Parse(ds.ID())
	if err != nil {
		return err
	}

	puller, err := datas.NewPuller(ctx, tempTableDir, defaultChunksPerTF, srcDB, destDB, stRef.TargetHash(), nil)
	if err == datas.ErrDBUpToDate {
		return nil
	} else if err != nil {
		return err
	}

	err = puller.Pull(ctx)
	if err != nil {
		return err
	}

	ds, err = destDB.GetDataset(ctx, rf.String())
	if err != nil {
		return err
	}

	_, err = destDB.SetHead(ctx, ds, stRef)
	return err
}
