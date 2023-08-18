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

package merge

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrFastForward = errors.New("fast forward")
var ErrTableDeletedAndModified = errors.New("conflict: table with same name deleted and modified ")
var ErrSchemaConflict = goerrors.NewKind("schema conflict found, merge aborted. Please alter schema to prevent schema conflicts before merging: %s")

// ErrCantOverwriteConflicts is returned when there are unresolved conflicts
// and the merge produces new conflicts. Because we currently don't have a model
// to merge sets of conflicts together, we need to abort the merge at this
// point.
var ErrCantOverwriteConflicts = errors.New("existing unresolved conflicts would be" +
	" overridden by new conflicts produced by merge. Please resolve them and try again")

var ErrConflictsIncompatible = errors.New("the existing conflicts are of a different schema" +
	" than the conflicts generated by this merge. Please resolve them and try again")

var ErrMultipleViolationsForRow = errors.New("multiple violations for row not supported")

var ErrSameTblAddedTwice = goerrors.NewKind("table with same name '%s' added in 2 commits can't be merged")

func MergeCommits(ctx *sql.Context, commit, mergeCommit *doltdb.Commit, opts editor.Options) (*Result, error) {
	ancCommit, err := doltdb.GetCommitAncestor(ctx, commit, mergeCommit)
	if err != nil {
		return nil, err
	}

	ourRoot, err := commit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	theirRoot, err := mergeCommit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	ancRoot, err := ancCommit.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	mo := MergeOpts{
		IsCherryPick:        false,
		KeepSchemaConflicts: true,
	}
	return MergeRoots(ctx, ourRoot, theirRoot, ancRoot, mergeCommit, ancCommit, opts, mo)
}

type Result struct {
	Root            *doltdb.RootValue
	SchemaConflicts []SchemaConflict
	Stats           map[string]*MergeStats
}

func (r Result) HasSchemaConflicts() bool {
	return len(r.SchemaConflicts) > 0
}

func (r Result) HasMergeArtifacts() bool {
	if r.HasSchemaConflicts() {
		return true
	}
	for _, stats := range r.Stats {
		if stats.HasArtifacts() {
			return true
		}
	}
	return false
}

// CountOfTablesWithDataConflicts returns the number of tables in this merge result that have
// a data conflict.
func (r Result) CountOfTablesWithDataConflicts() int {
	count := 0
	for _, mergeStats := range r.Stats {
		if mergeStats.HasDataConflicts() {
			count++
		}
	}
	return count
}

// CountOfTablesWithSchemaConflicts returns the number of tables in this merge result that have
// a schema conflict.
func (r Result) CountOfTablesWithSchemaConflicts() int {
	count := 0
	for _, mergeStats := range r.Stats {
		if mergeStats.HasSchemaConflicts() {
			count++
		}
	}
	return count
}

// CountOfTablesWithConstraintViolations returns the number of tables in this merge result that have
// a constraint violation.
func (r Result) CountOfTablesWithConstraintViolations() int {
	count := 0
	for _, mergeStats := range r.Stats {
		if mergeStats.HasConstraintViolations() {
			count++
		}
	}
	return count
}

func SchemaConflictTableNames(sc []SchemaConflict) (tables []string) {
	tables = make([]string, len(sc))
	for i := range sc {
		tables[i] = sc[i].TableName
	}
	return
}

// MergeRoots three-way merges |ourRoot|, |theirRoot|, and |ancRoot| and returns
// the merged root. If any conflicts or constraint violations are produced they
// are stored in the merged root. If |ourRoot| already contains conflicts they
// are stashed before the merge is performed. We abort the merge if the stash
// contains conflicts and we produce new conflicts. We currently don't have a
// model to merge conflicts together.
//
// Constraint violations that exist in ancestor are stashed and merged with the
// violations we detect when we diff the ancestor and the newly merged root.
//
// |theirRootIsh| is the hash of their's working set or commit. It is used to
// key any artifacts generated by this merge. |ancRootIsh| is similar and is
// used to retrieve the base value for a conflict.
func MergeRoots(
	ctx *sql.Context,
	ourRoot, theirRoot, ancRoot *doltdb.RootValue,
	theirs, ancestor doltdb.Rootish,
	opts editor.Options,
	mergeOpts MergeOpts,
) (*Result, error) {
	var (
		conflictStash  *conflictStash
		violationStash *violationStash
		nbf            *types.NomsBinFormat
		err            error
	)

	nbf = ourRoot.VRW().Format()
	if !types.IsFormat_DOLT(nbf) {
		ourRoot, conflictStash, err = stashConflicts(ctx, ourRoot)
		if err != nil {
			return nil, err
		}
		ancRoot, violationStash, err = stashViolations(ctx, ancRoot)
		if err != nil {
			return nil, err
		}
	}

	// Make sure to pass in ourRoot as the first RootValue so that ourRoot's table names will be merged first.
	// This helps to avoid non-deterministic error result for table rename cases. Renaming a table creates two changes:
	// 1. dropping the old name table
	// 2. adding the new name table
	// Dropping the old name table will trigger delete/modify conflict, which is the preferred error case over
	// same column tag used error returned from creating the new name table.
	tblNames, err := doltdb.UnionTableNames(ctx, ourRoot, theirRoot)

	if err != nil {
		return nil, err
	}

	tblToStats := make(map[string]*MergeStats)

	mergedRoot := ourRoot

	// Merge tables one at a time. This is done based on name. With table names from ourRoot being merged first,
	// renaming a table will return delete/modify conflict error consistently.
	// TODO: merge based on a more durable table identity that persists across renames
	merger, err := NewMerger(ourRoot, theirRoot, ancRoot, theirs, ancestor, ourRoot.VRW(), ourRoot.NodeStore())
	if err != nil {
		return nil, err
	}

	var schConflicts []SchemaConflict
	for _, tblName := range tblNames {
		mergedTable, stats, err := merger.MergeTable(ctx, tblName, opts, mergeOpts)
		if err != nil {
			// If a Full-Text table was both modified and deleted, then we want to ignore the deletion.
			// If there's a true conflict, then the parent table will catch the conflict.
			if doltdb.IsFullTextTable(tblName) && errors.Is(ErrTableDeletedAndModified, err) {
				stats = &MergeStats{Operation: TableModified}
			} else {
				return nil, err
			}
		}
		if doltdb.IsFullTextTable(tblName) && (stats.Operation == TableModified || stats.Operation == TableRemoved) {
			// We handle removal and modification later in the rebuilding process, so we'll skip those.
			// We do not handle adding new tables, so we allow that to proceed.
			continue
		}
		if mergedTable.conflict.Count() > 0 {
			if types.IsFormat_DOLT(nbf) {
				schConflicts = append(schConflicts, mergedTable.conflict)
			} else {
				// return schema conflict as error
				return nil, mergedTable.conflict
			}
		}

		if mergedTable.table != nil {
			tblToStats[tblName] = stats

			mergedRoot, err = mergedRoot.PutTable(ctx, tblName, mergedTable.table)
			if err != nil {
				return nil, err
			}
			continue
		}

		newRootHasTable, err := mergedRoot.HasTable(ctx, tblName)
		if err != nil {
			return nil, err
		}

		if newRootHasTable {
			// Merge root deleted this table
			tblToStats[tblName] = &MergeStats{Operation: TableRemoved}

			mergedRoot, err = mergedRoot.RemoveTables(ctx, false, false, tblName)
			if err != nil {
				return nil, err
			}
		} else {
			// This is a deleted table that the merge root still has
			if stats.Operation != TableRemoved {
				panic(fmt.Sprintf("Invalid merge state for table %s. This is a bug.", tblName))
			}
			// Nothing to update, our root already has the table deleted
		}
	}

	mergedRoot, err = rebuildFullTextIndexes(ctx, mergedRoot)
	if err != nil {
		return nil, err
	}

	mergedFKColl, conflicts, err := ForeignKeysMerge(ctx, mergedRoot, ourRoot, theirRoot, ancRoot)
	if err != nil {
		return nil, err
	}
	if len(conflicts) > 0 {
		return nil, fmt.Errorf("foreign key conflicts")
	}

	mergedRoot, err = mergedRoot.PutForeignKeyCollection(ctx, mergedFKColl)
	if err != nil {
		return nil, err
	}

	h, err := merger.rightSrc.HashOf()
	if err != nil {
		return nil, err
	}

	mergedRoot, _, err = AddForeignKeyViolations(ctx, mergedRoot, ancRoot, nil, h)
	if err != nil {
		return nil, err
	}

	if types.IsFormat_DOLT(ourRoot.VRW().Format()) {
		err = getConstraintViolationStats(ctx, mergedRoot, tblToStats)
		if err != nil {
			return nil, err
		}

		return &Result{
			Root:            mergedRoot,
			SchemaConflicts: schConflicts,
			Stats:           tblToStats,
		}, nil
	}

	mergedRoot, err = mergeCVsWithStash(ctx, mergedRoot, violationStash)
	if err != nil {
		return nil, err
	}

	err = getConstraintViolationStats(ctx, mergedRoot, tblToStats)
	if err != nil {
		return nil, err
	}

	mergedHasConflicts := checkForConflicts(tblToStats)
	if !conflictStash.Empty() && mergedHasConflicts {
		return nil, ErrCantOverwriteConflicts
	} else if !conflictStash.Empty() {
		mergedRoot, err = applyConflictStash(ctx, conflictStash.Stash, mergedRoot)
		if err != nil {
			return nil, err
		}
	}

	return &Result{
		Root:            mergedRoot,
		SchemaConflicts: schConflicts,
		Stats:           tblToStats,
	}, nil
}

// mergeCVsWithStash merges the table constraint violations in |stash| with |root|.
// Returns an updated root with all the merged CVs.
func mergeCVsWithStash(ctx context.Context, root *doltdb.RootValue, stash *violationStash) (*doltdb.RootValue, error) {
	updatedRoot := root
	for name, stashed := range stash.Stash {
		tbl, ok, err := root.GetTable(ctx, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			// the table with the CVs was deleted
			continue
		}
		curr, err := tbl.GetConstraintViolations(ctx)
		if err != nil {
			return nil, err
		}
		unioned, err := types.UnionMaps(ctx, curr, stashed, func(key types.Value, currV types.Value, stashV types.Value) (types.Value, error) {
			if !currV.Equals(stashV) {
				panic(fmt.Sprintf("encountered conflict when merging constraint violations, conflicted key: %v\ncurrent value: %v\nstashed value: %v\n", key, currV, stashV))
			}
			return currV, nil
		})
		if err != nil {
			return nil, err
		}
		tbl, err = tbl.SetConstraintViolations(ctx, unioned)
		if err != nil {
			return nil, err
		}
		updatedRoot, err = root.PutTable(ctx, name, tbl)
		if err != nil {
			return nil, err
		}
	}
	return updatedRoot, nil
}

// checks if a conflict occurred during the merge
func checkForConflicts(tblToStats map[string]*MergeStats) bool {
	for _, stat := range tblToStats {
		if stat.HasConflicts() {
			return true
		}
	}
	return false
}

// populates tblToStats with violation statistics
func getConstraintViolationStats(ctx context.Context, root *doltdb.RootValue, tblToStats map[string]*MergeStats) error {
	for tblName, stats := range tblToStats {
		tbl, ok, err := root.GetTable(ctx, tblName)
		if err != nil {
			return err
		}
		if ok {
			n, err := tbl.NumConstraintViolations(ctx)
			if err != nil {
				return err
			}
			stats.ConstraintViolations = int(n)
		}
	}
	return nil
}

// MayHaveConstraintViolations returns whether the given roots may have constraint violations. For example, a fast
// forward merge that does not involve any tables with foreign key constraints or check constraints will not be able
// to generate constraint violations. Unique key constraint violations would be caught during the generation of the
// merged root, therefore it is not a factor for this function.
func MayHaveConstraintViolations(ctx context.Context, ancestor, merged *doltdb.RootValue) (bool, error) {
	ancTables, err := ancestor.MapTableHashes(ctx)
	if err != nil {
		return false, err
	}
	mergedTables, err := merged.MapTableHashes(ctx)
	if err != nil {
		return false, err
	}
	fkColl, err := merged.GetForeignKeyCollection(ctx)
	if err != nil {
		return false, err
	}
	tablesInFks := fkColl.Tables()
	for tblName := range tablesInFks {
		if ancHash, ok := ancTables[tblName]; !ok {
			// If a table used in a foreign key is new then it's treated as a change
			return true, nil
		} else if mergedHash, ok := mergedTables[tblName]; !ok {
			return false, fmt.Errorf("foreign key uses table '%s' but no hash can be found for this table", tblName)
		} else if !ancHash.Equal(mergedHash) {
			return true, nil
		}
	}
	return false, nil
}

type ArtifactStatus struct {
	SchemaConflictsTables      []string
	DataConflictTables         []string
	ConstraintViolationsTables []string
}

func (as ArtifactStatus) HasConflicts() bool {
	return len(as.DataConflictTables) > 0 || len(as.SchemaConflictsTables) > 0
}

func (as ArtifactStatus) HasConstraintViolations() bool {
	return len(as.ConstraintViolationsTables) > 0
}

func GetMergeArtifactStatus(ctx context.Context, working *doltdb.WorkingSet) (as ArtifactStatus, err error) {
	if working.MergeActive() {
		as.SchemaConflictsTables = working.MergeState().TablesWithSchemaConflicts()
	}

	as.DataConflictTables, err = working.WorkingRoot().TablesWithDataConflicts(ctx)
	if err != nil {
		return as, err
	}

	as.ConstraintViolationsTables, err = working.WorkingRoot().TablesWithConstraintViolations(ctx)
	if err != nil {
		return as, err
	}
	return
}

// MergeWouldStompChanges returns list of table names that are stomped and the diffs map between head and working set.
func MergeWouldStompChanges(ctx context.Context, roots doltdb.Roots, mergeCommit *doltdb.Commit) ([]string, map[string]hash.Hash, error) {
	mergeRoot, err := mergeCommit.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	headTableHashes, err := roots.Head.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	workingTableHashes, err := roots.Working.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeTableHashes, err := mergeRoot.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	headWorkingDiffs := diffTableHashes(headTableHashes, workingTableHashes)
	mergedHeadDiffs := diffTableHashes(headTableHashes, mergeTableHashes)

	stompedTables := make([]string, 0, len(headWorkingDiffs))
	for tName, _ := range headWorkingDiffs {
		if _, ok := mergedHeadDiffs[tName]; ok {
			// even if the working changes match the merge changes, don't allow (matches git behavior).
			stompedTables = append(stompedTables, tName)
		}
	}

	return stompedTables, headWorkingDiffs, nil
}

func diffTableHashes(headTableHashes, otherTableHashes map[string]hash.Hash) map[string]hash.Hash {
	diffs := make(map[string]hash.Hash)
	for tName, hh := range headTableHashes {
		if h, ok := otherTableHashes[tName]; ok {
			if h != hh {
				// modification
				diffs[tName] = h
			}
		} else {
			// deletion
			diffs[tName] = hash.Hash{}
		}
	}

	for tName, h := range otherTableHashes {
		if _, ok := headTableHashes[tName]; !ok {
			// addition
			diffs[tName] = h
		}
	}

	return diffs
}
