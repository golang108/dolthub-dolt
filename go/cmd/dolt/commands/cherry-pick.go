// Copyright 2022 Dolthub, Inc.
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

package commands

import (
	"context"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var cherryPickDocs = cli.CommandDocumentationContent{
	ShortDesc: `Apply the changes introduced by an existing commit.`,
	LongDesc: `
Updates tables in the clean working set with changes introduced in cherry-picked commit and creates a new commit with applied changes.
Currently, schema changes introduced in cherry-pick commit are not supported. Row data changes are allowed with a table schema in working set 
matching a table schema in cherry-pick commit with the same name. If there is a conflict, the working state stays clean.
Cherry-picking a merge commit or cherry-picked commit is not supported.

dolt cherry-pick {{.LessThan}}commit{{.GreaterThan}}
   To apply changes from an existing {{.LessThan}}commit{{.GreaterThan}} to current HEAD, the current working tree must be clean (no modifications from the HEAD commit). 
   By default, cherry-pick creates new commit with applied changes.`,
	Synopsis: []string{
		`{{.LessThan}}commit{{.GreaterThan}}`,
	},
}

type CherryPickCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command.
func (cmd CherryPickCmd) Name() string {
	return "cherry-pick"
}

// Description returns a description of the command.
func (cmd CherryPickCmd) Description() string {
	return "Apply the changes introduced by an existing commit."
}

func (cmd CherryPickCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateCheckoutArgParser()
	return cli.NewCommandDocumentation(cherryPickDocs, ap)
}

func (cmd CherryPickCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCherryPickArgParser()
}

// EventType returns the type of the event to log.
func (cmd CherryPickCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CHERRY_PICK
}

// Exec executes the command.
func (cmd CherryPickCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateCherryPickArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cherryPickDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	// This command creates a commit, so we need user identity
	if !cli.CheckUserNameAndEmail(dEnv) {
		return 1
	}

	// TODO : support single commit cherry-pick only for now
	if apr.NArg() == 0 {
		usage()
		return 1
	} else if apr.NArg() > 1 {
		return HandleVErrAndExitCode(errhand.BuildDError("multiple commits not supported yet.").SetPrintUsage().Build(), usage)
	}

	cherryStr := apr.Arg(0)
	if len(cherryStr) == 0 {
		verr := errhand.BuildDError("error: cannot cherry-pick empty string").Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	authorStr := ""
	if as, ok := apr.GetValue(cli.AuthorParam); ok {
		authorStr = as
	}

	verr := cherryPick(ctx, dEnv, cherryStr, authorStr)
	if verr != nil {
		cli.PrintErrln("fatal: cherry-pick failed")
	}
	return HandleVErrAndExitCode(verr, usage)
}

// cherryPick returns error if any step of cherry-picking fails. It receives cherry-picked commit and performs cherry-picking and commits.
func cherryPick(ctx context.Context, dEnv *env.DoltEnv, cherryStr, authorStr string) errhand.VerboseError {
	// check for clean working state
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	stagedHash, err := stagedRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	if !stagedHash.Equal(workingHash) {
		return errhand.BuildDError("error: your local changes would be overwritten by cherry-pick.\n commit you changes or ").Build()
	}

	// TODO : git functionality seems like it merges unless there is conflict???
	if !headHash.Equal(workingHash) {
		return errhand.BuildDError("You must commit any changes before using cherry-pick.").Build()
	}

	newWorkingRoot, commitMsg, err := getCherryPickedRootValue(ctx, dEnv, workingRoot, headHash, cherryStr)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	workingHash, err = newWorkingRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	if headHash.Equal(workingHash) {
		cli.Println("No changes were made.")
		return nil
	}

	err = dEnv.UpdateWorkingRoot(ctx, newWorkingRoot)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	res := AddCmd{}.Exec(ctx, "add", []string{"-A"}, dEnv)
	if res != 0 {
		return errhand.BuildDError("dolt add failed").AddCause(err).Build()
	}

	// Pass in the final parameters for the author string.
	commitParams := []string{"-m", commitMsg}
	if authorStr != "" {
		commitParams = append(commitParams, "--author", authorStr)
	}

	res = CommitCmd{}.Exec(ctx, "commit", commitParams, dEnv)
	if res != 0 {
		return errhand.BuildDError("dolt commit failed").AddCause(err).Build()
	}

	return nil
}

// getCherryPickedRootValue returns updated RootValue for current HEAD after cherry-pick commit is merged successfully and
// commit message of cherry-picked commit.
func getCherryPickedRootValue(ctx context.Context, dEnv *env.DoltEnv, workingRoot *doltdb.RootValue, headHash hash.Hash, cherryStr string) (*doltdb.RootValue, string, error) {
	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}

	cherrySpec, err := doltdb.NewCommitSpec(cherryStr)
	if err != nil {
		return nil, "", err
	}
	cherryCommit, err := dEnv.DoltDB.Resolve(ctx, cherrySpec, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return nil, "", err
	}

	cherryCM, err := cherryCommit.GetCommitMeta(ctx)
	if err != nil {
		return nil, "", err
	}
	commitMsg := cherryCM.Description

	fromRoot, toRoot, err := getParentAndCherryRoots(ctx, dEnv.DoltDB, cherryCommit)
	if err != nil {
		return nil, "", errhand.BuildDError("failed to get cherry-picked commit and its parent commit").AddCause(err).Build()
	}
	fromHash, err := fromRoot.HashOf()
	if err != nil {
		return nil, "", err
	}
	toHash, err := toRoot.HashOf()
	if err != nil {
		return nil, "", err
	}

	// use parent of cherry-pick as ancestor to merge
	mergedRoot, mergeStat, err := merge.MergeRoots(ctx, toHash, fromHash, workingRoot, toRoot, fromRoot, opts, true)
	if err != nil {
		return nil, "", err
	}

	for _, stats := range mergeStat {
		if stats.Conflicts != 0 {
			//cli.Println("conflict here")
			return nil, "", errhand.BuildDError("conflict occurred").Build()
		}
	}

	return mergedRoot, commitMsg, nil
}

// getParentAndCherryRoots return root values of parent commit of cherry-picked commit and cherry-picked commit itself.
func getParentAndCherryRoots(ctx context.Context, ddb *doltdb.DoltDB, cherryCommit *doltdb.Commit) (*doltdb.RootValue, *doltdb.RootValue, error) {
	cherryRoot, err := cherryCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	var parentRoot *doltdb.RootValue
	if len(cherryCommit.DatasParents()) > 1 {
		return nil, nil, errhand.BuildDError("cherry-picking a merge or cherry-picked commit is not supported.").Build()
	} else if len(cherryCommit.DatasParents()) == 1 {
		parentCM, err := ddb.ResolveParent(ctx, cherryCommit, 0)
		if err != nil {
			return nil, nil, err
		}
		parentRoot, err = parentCM.GetRootValue(ctx)
		if err != nil {
			return nil, nil, err
		}
	} else {
		parentRoot, err = doltdb.EmptyRootValue(ctx, ddb.ValueReadWriter())
		if err != nil {
			return nil, nil, err
		}
	}
	return parentRoot, cherryRoot, nil
}
