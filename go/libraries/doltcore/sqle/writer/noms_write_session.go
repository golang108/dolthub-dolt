// Copyright 2020 Dolthub, Inc.
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

package writer

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// WriteSession encapsulates writes made within a SQL session.
// It's responsible for creating and managing the lifecycle of TableWriter's.
type WriteSession interface {
	// GetTableWriter creates a TableWriter and adds it to the WriteSession.
	GetTableWriter(ctx context.Context, table, db string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (TableWriter, error)

	// UpdateRoot takes a callback to update this WriteSession's root. WriteSession flushes
	// the pending writes in the session before calling the callback.
	UpdateRoot(ctx context.Context, cb func(ctx context.Context, current *doltdb.RootValue) (*doltdb.RootValue, error)) error

	// SetRoot sets the root for the WriteSession.
	SetRoot(ctx context.Context, root *doltdb.RootValue) error

	// Flush flushes the pending writes in the session.
	Flush(ctx context.Context) (*doltdb.RootValue, error)

	// GetOptions returns the editor.Options for this session.
	GetOptions() editor.Options

	// SetOptions sets the editor.Options for this session.
	SetOptions(opts editor.Options)
}

// nomsWriteSession handles all edit operations on a table that may also update other tables.
// Serves as coordination for SessionedTableEditors.
type nomsWriteSession struct {
	root   *doltdb.RootValue
	tables map[string]*sessionedTableEditor
	autos  map[string]globalstate.AutoIncrementTracker
	mut    *sync.RWMutex // This mutex is specifically for changes that affect the TES or all STEs

	opts editor.Options
}

var _ WriteSession = &nomsWriteSession{}

// NewWriteSession creates and returns a WriteSession. Inserting a nil root is not an error, as there are
// locations that do not have a root at the time of this call. However, a root must be set through SetRoot before any
// table editors are returned.
func NewWriteSession(nbf *types.NomsBinFormat, root *doltdb.RootValue, opts editor.Options) WriteSession {
	if types.IsFormat_DOLT_1(nbf) {
		return &prollyWriteSession{
			root:   root,
			tables: make(map[string]*prollyTableWriter),
			mut:    &sync.RWMutex{},
		}
	}

	return &nomsWriteSession{
		opts:   opts,
		root:   root,
		tables: make(map[string]*sessionedTableEditor),
		autos:  make(map[string]globalstate.AutoIncrementTracker),
		mut:    &sync.RWMutex{},
	}
}

func (s *nomsWriteSession) GetTableWriter(ctx context.Context, table string, database string, ait globalstate.AutoIncrementTracker, setter SessionRootSetter, batched bool) (TableWriter, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	t, ok, err := s.root.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, doltdb.ErrTableNotFound
	}

	sch, err := t.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	te, err := s.getTableEditor(ctx, table, sch)
	if err != nil {
		return nil, err
	}
	s.autos[table] = ait

	conv := index.NewKVToSqlRowConverterForCols(t.Format(), sch)

	autoOrd := -1
	for i, col := range sch.GetAllCols().GetColumns() {
		if col.AutoIncrement {
			autoOrd = i
			break
		}
	}

	return &nomsTableWriter{
		tableName:   table,
		dbName:      database,
		sch:         sch,
		vrw:         s.root.VRW(),
		kvToSQLRow:  conv,
		tableEditor: te,
		sess:        s,
		batched:     batched,
		autoInc:     ait,
		autoOrd:     autoOrd,
		setter:      setter,
	}, nil
}

// Flush returns an updated root with all of the changed tables.
func (s *nomsWriteSession) Flush(ctx context.Context) (*doltdb.RootValue, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.flush(ctx)
}

// SetRoot uses the given root to set all open table editors to the state as represented in the root. If any
// tables are removed in the root, but have open table editors, then the references to those are removed. If those
// removed table's editors are used after this, then the behavior is undefined. This will lose any changes that have not
// been flushed. If the purpose is to add a new table, foreign key, etc. (using Flush followed up with SetRoot), then
// use UpdateRoot. Calling the two functions manually for the purposes of root modification may lead to race conditions.
func (s *nomsWriteSession) SetRoot(ctx context.Context, root *doltdb.RootValue) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	return s.setRoot(ctx, root)
}

// UpdateRoot takes in a function meant to update the root (whether that be updating a table's schema, adding a foreign
// key, etc.) and passes in the flushed root. The function may then safely modify the root, and return the modified root
// (assuming no errors). The nomsWriteSession will update itself in accordance with the newly returned root.
func (s *nomsWriteSession) UpdateRoot(ctx context.Context, cb func(ctx context.Context, current *doltdb.RootValue) (*doltdb.RootValue, error)) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	current, err := s.flush(ctx)
	if err != nil {
		return err
	}

	mutated, err := cb(ctx, current)
	if err != nil {
		return err
	}

	return s.setRoot(ctx, mutated)
}

func (s *nomsWriteSession) GetOptions() editor.Options {
	return s.opts
}

func (s *nomsWriteSession) SetOptions(opts editor.Options) {
	s.opts = opts
}

// flush is the inner implementation for Flush that does not acquire any locks
func (s *nomsWriteSession) flush(ctx context.Context) (*doltdb.RootValue, error) {
	newRoot := s.root
	mu := &sync.Mutex{}
	rootUpdate := func(name string, table *doltdb.Table) (err error) {
		mu.Lock()
		defer mu.Unlock()
		if newRoot != nil {
			newRoot, err = newRoot.PutTable(ctx, name, table)
		}
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)

	for tblName, tblEditor := range s.tables {
		if !tblEditor.HasEdits() {
			continue
		}

		// copy variables
		name, ed := tblName, tblEditor

		eg.Go(func() error {
			tbl, err := ed.tableEditor.Table(ctx)
			if err != nil {
				return err
			}

			v := s.autos[name].Current(name)
			tbl, err = tbl.SetAutoIncrementValue(ctx, v)
			if err != nil {
				return err
			}

			return rootUpdate(name, tbl)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	s.root = newRoot
	return newRoot, nil
}

// getTableEditor is the inner implementation for GetTableEditor, allowing recursive calls
func (s *nomsWriteSession) getTableEditor(ctx context.Context, tableName string, tableSch schema.Schema) (*sessionedTableEditor, error) {
	if s.root == nil {
		return nil, fmt.Errorf("must call SetRoot before a table editor will be returned")
	}

	var t *doltdb.Table
	var err error
	localTableEditor, ok := s.tables[tableName]
	if ok {
		if tableSch == nil {
			return localTableEditor, nil
		} else if schema.SchemasAreEqual(tableSch, localTableEditor.tableEditor.Schema()) {
			return localTableEditor, nil
		}
		// Any existing references to this localTableEditor should be preserved, so we just change the underlying values
		localTableEditor.referencedTables = nil
		localTableEditor.referencingTables = nil
	} else {
		localTableEditor = &sessionedTableEditor{
			tableEditSession:  s,
			tableEditor:       nil,
			referencedTables:  nil,
			referencingTables: nil,
		}
		s.tables[tableName] = localTableEditor
	}

	t, ok, err = s.root.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unable to create table editor as `%s` is missing", tableName)
	}
	if tableSch == nil {
		tableSch, err = t.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	tableEditor, err := editor.NewTableEditor(ctx, t, tableSch, tableName, s.opts)
	if err != nil {
		return nil, err
	}

	localTableEditor.tableEditor = tableEditor

	if s.opts.ForeignKeyChecksDisabled {
		return localTableEditor, nil
	}

	fkCollection, err := s.root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}
	localTableEditor.referencedTables, localTableEditor.referencingTables = fkCollection.KeysForTable(tableName)
	err = s.loadForeignKeys(ctx, localTableEditor)
	if err != nil {
		return nil, err
	}

	return localTableEditor, nil
}

// loadForeignKeys loads all tables mentioned in foreign keys for the given editor
func (s *nomsWriteSession) loadForeignKeys(ctx context.Context, localTableEditor *sessionedTableEditor) error {
	// these are the tables that reference us, so we need to update them
	for _, foreignKey := range localTableEditor.referencingTables {
		if !foreignKey.IsResolved() {
			continue
		}
		_, err := s.getTableEditor(ctx, foreignKey.TableName, nil)
		if err != nil {
			return err
		}
	}
	// these are the tables that we reference, so we need to refer to them
	for _, foreignKey := range localTableEditor.referencedTables {
		if !foreignKey.IsResolved() {
			continue
		}
		_, err := s.getTableEditor(ctx, foreignKey.ReferencedTableName, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// setRoot is the inner implementation for SetRoot that does not acquire any locks
func (s *nomsWriteSession) setRoot(ctx context.Context, root *doltdb.RootValue) error {
	if root == nil {
		return fmt.Errorf("cannot set a nomsWriteSession's root to nil once it has been created")
	}

	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	s.root = root

	all, _ := root.AllAutoIncrements(ctx)
	fmt.Println(all)

	for tableName, localTableEditor := range s.tables {
		t, ok, err := root.GetTable(ctx, tableName)
		if err != nil {
			return err
		}
		if !ok { // table was removed in newer root
			if err := localTableEditor.tableEditor.Close(ctx); err != nil {
				return err
			}
			delete(s.tables, tableName)
			continue
		}
		tSch, err := t.GetSchema(ctx)
		if err != nil {
			return err
		}

		if schema.HasAutoIncrement(tSch) {
			seq, err := t.GetAutoIncrementValue(ctx)
			if err != nil {
				return err
			}
			// todo(andy): do we need some monotonicity here?
			s.autos[tableName].Set(tableName, seq)
		}

		newTableEditor, err := editor.NewTableEditor(ctx, t, tSch, tableName, s.opts)
		if err != nil {
			return err
		}
		if err := localTableEditor.tableEditor.Close(ctx); err != nil {
			return err
		}
		localTableEditor.tableEditor = newTableEditor
		localTableEditor.referencedTables, localTableEditor.referencingTables = fkCollection.KeysForTable(tableName)
		if !s.opts.ForeignKeyChecksDisabled {
			err = s.loadForeignKeys(ctx, localTableEditor)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
