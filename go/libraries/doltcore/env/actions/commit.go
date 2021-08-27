// Copyright 2019 Dolthub, Inc.
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

package actions

import (
	"bytes"
	"context"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/editor"
	"github.com/dolthub/dolt/go/store/hash"
)

type CommitStagedProps struct {
	Message    string
	Date       time.Time
	AllowEmpty bool
	Force      bool
	Name       string
	Email      string
}

// GetNameAndEmail returns the name and email from the supplied config
func GetNameAndEmail(cfg config.ReadableConfig) (string, string, error) {
	name, err := cfg.GetString(env.UserNameKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", doltdb.ErrNameNotConfigured
	} else if err != nil {
		return "", "", err
	}

	email, err := cfg.GetString(env.UserEmailKey)

	if err == config.ErrConfigParamNotFound {
		return "", "", doltdb.ErrEmailNotConfigured
	} else if err != nil {
		return "", "", err
	}

	return name, email, nil
}

// CommitStaged adds a new commit to HEAD with the given props. Returns the new commit's hash as a string and an error.
func CommitStaged(ctx context.Context, roots doltdb.Roots, mergeActive bool, mergeParents []*doltdb.Commit, dbData env.DbData, props CommitStagedProps) (*doltdb.Commit, error) {
	ddb := dbData.Ddb
	rsr := dbData.Rsr
	rsw := dbData.Rsw
	drw := dbData.Drw

	if props.Message == "" {
		return nil, doltdb.ErrEmptyCommitMessage
	}

	staged, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	var stagedTblNames []string
	for _, td := range staged {
		n := td.ToName
		if td.IsDrop() {
			n = td.FromName
		}
		stagedTblNames = append(stagedTblNames, n)
	}

	if len(staged) == 0 && !mergeActive && !props.AllowEmpty {
		_, notStagedDocs, err := diff.GetDocDiffs(ctx, roots, drw)
		if err != nil {
			return nil, err
		}
		return nil, NothingStaged{notStaged, notStagedDocs}
	}

	if !props.Force {
		inConflict, err := roots.Working.TablesInConflict(ctx)
		if err != nil {
			return nil, err
		}
		if len(inConflict) > 0 {
			return nil, NewTblInConflictError(inConflict)
		}
		violatesConstraints, err := roots.Working.TablesWithConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}
		if len(violatesConstraints) > 0 {
			return nil, NewTblHasConstraintViolations(violatesConstraints)
		}
	}

	stagedRoot, err := roots.Staged.UpdateSuperSchemasFromOther(ctx, stagedTblNames, roots.Staged)
	if err != nil {
		return nil, err
	}

	if !props.Force {
		stagedRoot, err = stagedRoot.ValidateForeignKeysOnSchemas(ctx)
		if err != nil {
			return nil, err
		}
	}

	// TODO: combine into a single update
	err = rsw.UpdateStagedRoot(ctx, stagedRoot)
	if err != nil {
		return nil, err
	}

	workingRoot, err := roots.Working.UpdateSuperSchemasFromOther(ctx, stagedTblNames, stagedRoot)
	if err != nil {
		return nil, err
	}

	err = rsw.UpdateWorkingRoot(ctx, workingRoot)
	if err != nil {
		return nil, err
	}

	meta, err := doltdb.NewCommitMetaWithUserTS(props.Name, props.Email, props.Message, props.Date)
	if err != nil {
		return nil, err
	}

	// TODO: this is only necessary in some contexts (SQL). Come up with a more coherent set of interfaces to
	//  rationalize where the root value writes happen before a commit is created.
	h, err := ddb.WriteRootValue(ctx, stagedRoot)
	if err != nil {
		return nil, err
	}

	// logrus.Errorf("staged root is %s", stagedRoot.DebugString(ctx, true))

	// DoltDB resolves the current working branch head ref to provide a parent commit.
	c, err := ddb.CommitWithParentCommits(ctx, h, rsr.CWBHeadRef(), mergeParents, meta)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// TimeSortedCommits returns a reverse-chronological (latest-first) list of the most recent `n` ancestors of `commit`.
// Passing a negative value for `n` will result in all ancestors being returned.
func TimeSortedCommits(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, n int) ([]*doltdb.Commit, error) {
	hashToCommit := make(map[hash.Hash]*doltdb.Commit)
	err := AddCommits(ctx, ddb, commit, hashToCommit, n)

	if err != nil {
		return nil, err
	}

	idx := 0
	uniqueCommits := make([]*doltdb.Commit, len(hashToCommit))
	for _, v := range hashToCommit {
		uniqueCommits[idx] = v
		idx++
	}

	var sortErr error
	var metaI, metaJ *doltdb.CommitMeta
	sort.Slice(uniqueCommits, func(i, j int) bool {
		if sortErr != nil {
			return false
		}

		metaI, sortErr = uniqueCommits[i].GetCommitMeta()

		if sortErr != nil {
			return false
		}

		metaJ, sortErr = uniqueCommits[j].GetCommitMeta()

		if sortErr != nil {
			return false
		}

		return metaI.UserTimestamp > metaJ.UserTimestamp
	})

	if sortErr != nil {
		return nil, sortErr
	}

	return uniqueCommits, nil
}

func AddCommits(ctx context.Context, ddb *doltdb.DoltDB, commit *doltdb.Commit, hashToCommit map[hash.Hash]*doltdb.Commit, n int) error {
	hash, err := commit.HashOf()

	if err != nil {
		return err
	}

	if _, ok := hashToCommit[hash]; ok {
		return nil
	}

	hashToCommit[hash] = commit

	numParents, err := commit.NumParents()

	if err != nil {
		return err
	}

	for i := 0; i < numParents && len(hashToCommit) != n; i++ {
		parentCommit, err := ddb.ResolveParent(ctx, commit, i)

		if err != nil {
			return err
		}

		err = AddCommits(ctx, ddb, parentCommit, hashToCommit, n)

		if err != nil {
			return err
		}
	}

	return nil
}

func GetCommitMessageFromEditor(ctx context.Context, dEnv *env.DoltEnv) string {
	var finalMsg string
	initialMsg := buildInitalCommitMsg(ctx, dEnv)
	backupEd := "vim"
	if ed, edSet := os.LookupEnv("EDITOR"); edSet {
		backupEd = ed
	}
	editorStr := dEnv.Config.GetStringOrDefault(env.DoltEditor, backupEd)

	cli.ExecuteWithStdioRestored(func() {
		commitMsg, _ := editor.OpenCommitEditor(*editorStr, initialMsg)
		finalMsg = parseCommitMessage(commitMsg)
	})
	return finalMsg
}

// TODO: return an error here
func buildInitalCommitMsg(ctx context.Context, dEnv *env.DoltEnv) string {
	initialNoColor := color.NoColor
	color.NoColor = true

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		panic(err)
	}

	stagedTblDiffs, notStagedTblDiffs, _ := diff.GetStagedUnstagedTableDeltas(ctx, roots)

	workingTblsInConflict, _, _, err := merge.GetTablesInConflict(ctx, roots)
	if err != nil {
		workingTblsInConflict = []string{}
	}
	workingTblsWithViolations, _, _, err := merge.GetTablesWithConstraintViolations(ctx, roots)
	if err != nil {
		workingTblsWithViolations = []string{}
	}

	stagedDocDiffs, notStagedDocDiffs, _ := diff.GetDocDiffs(ctx, roots, dEnv.DocsReadWriter())

	buf := bytes.NewBuffer([]byte{})
	n := printStagedDiffs(buf, stagedTblDiffs, stagedDocDiffs, true)
	n = PrintDiffsNotStaged(ctx, dEnv, buf, notStagedTblDiffs, notStagedDocDiffs, true, n, workingTblsInConflict, workingTblsWithViolations)

	currBranch := dEnv.RepoStateReader().CWBHeadRef()
	initialCommitMessage := "\n" + "# Please enter the commit message for your changes. Lines starting" + "\n" +
		"# with '#' will be ignored, and an empty message aborts the commit." + "\n# On branch " + currBranch.GetPath() + "\n#" + "\n"

	msgLines := strings.Split(buf.String(), "\n")
	for i, msg := range msgLines {
		msgLines[i] = "# " + msg
	}
	statusMsg := strings.Join(msgLines, "\n")

	color.NoColor = initialNoColor
	return initialCommitMessage + statusMsg
}

func parseCommitMessage(cm string) string {
	lines := strings.Split(cm, "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if len(line) >= 1 && line[0] == '#' {
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.Join(filtered, "\n")
}
