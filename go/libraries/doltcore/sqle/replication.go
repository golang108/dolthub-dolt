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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

func getPushOnWriteHook(ctx context.Context, dEnv *env.DoltEnv) (datas.CommitHook, error) {
	_, val, ok := sql.SystemVariables.GetGlobal(ReplicateToRemoteKey)
	if !ok {
		return nil, sql.ErrUnknownSystemVariable.New(ReplicateToRemoteKey)
	} else if val == "" {
		return nil, nil
	}

	remoteName, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidSystemVariableValue.New(val)
	}

	remotes, err := dEnv.GetRemotes()
	if err != nil {
		return nil, err
	}

	rem, ok := remotes[remoteName]
	if !ok {
		return nil, fmt.Errorf("%w: '%s'", env.ErrRemoteNotFound, remoteName)
	}

	ddb, err := rem.GetRemoteDB(ctx, types.Format_Default)
	if err != nil {
		return nil, err
	}

	if _, val, ok := sql.SystemVariables.GetGlobal(AsyncReplicationKey); ok && val == int8(1) {
		return doltdb.NewAsyncPushOnWriteHook(ctx, ddb, dEnv.TempTableFilesDir()), nil
	}

	return doltdb.NewPushOnWriteHook(ddb, dEnv.TempTableFilesDir()), nil
}

// GetCommitHooks creates a list of hooks to execute on database commit. If doltdb.SkipReplicationErrorsKey is set,
// replace misconfigured hooks with doltdb.LogHook instances that prints a warning when trying to execute.
func GetCommitHooks(ctx context.Context, dEnv *env.DoltEnv) ([]datas.CommitHook, error) {
	postCommitHooks := make([]datas.CommitHook, 0)

	if hook, err := getPushOnWriteHook(ctx, dEnv); err != nil {
		err = fmt.Errorf("failure loading hook; %w", err)
		if SkipReplicationWarnings() {
			postCommitHooks = append(postCommitHooks, doltdb.NewLogHook([]byte(err.Error()+"\n")))
		} else {
			return nil, err
		}
	} else if hook != nil {
		postCommitHooks = append(postCommitHooks, hook)
	}

	return postCommitHooks, nil
}

// newReplicaDatabase creates a new dsqle.ReadReplicaDatabase. If the doltdb.SkipReplicationErrorsKey global variable is set,
// skip errors related to database construction only and return a partially functional dsqle.ReadReplicaDatabase
// that will log warnings when attempting to perform replica commands.
func newReplicaDatabase(ctx context.Context, name string, remoteName string, dEnv *env.DoltEnv) (ReadReplicaDatabase, error) {
	opts := editor.Options{
		Deaf: dEnv.DbEaFactory(),
	}

	db := NewDatabase(name, dEnv.DbData(), opts)

	rrd, err := NewReadReplicaDatabase(ctx, db, remoteName, dEnv.RepoStateReader(), dEnv.TempTableFilesDir())
	if err != nil {
		err = fmt.Errorf("%w from remote '%s'; %s", ErrFailedToLoadReplicaDB, remoteName, err.Error())
		if !SkipReplicationWarnings() {
			return ReadReplicaDatabase{}, err
		}
		cli.Println(err)
		return ReadReplicaDatabase{Database: db}, nil
	}
	return rrd, nil
}

func applyReplicationConfig(ctx context.Context, mrEnv *env.MultiRepoEnv, dbs map[string]sql.Database) (map[string]sql.Database, error) {
	outputDbs := make(map[string]sql.Database, len(dbs))
	err := mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
		postCommitHooks, err := GetCommitHooks(ctx, dEnv)
		if err != nil {
			return true, err
		}
		dEnv.DoltDB.SetCommitHooks(ctx, postCommitHooks)

		db, ok := dbs[name]
		if !ok {
			//return true, sql.ErrDatabaseNotFound.New(name)
			return false, nil
		}

		if _, remote, ok := sql.SystemVariables.GetGlobal(ReadReplicaRemoteKey); ok && remote != "" {
			remoteName, ok := remote.(string)
			if !ok {
				return true, sql.ErrInvalidSystemVariableValue.New(remote)
			}
			db, err = newReplicaDatabase(ctx, name, remoteName, dEnv)
			if err != nil {
				return true, err
			}
		}

		outputDbs[name] = db

		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return outputDbs, nil
}
