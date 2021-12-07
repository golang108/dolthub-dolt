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

package mvdata

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

type CsvOptions struct {
	Delim string
}

type XlsxOptions struct {
	SheetName string
}

type JSONOptions struct {
	TableName string
	SchFile   string
}

type ParquetOptions struct {
	TableName string
	SchFile   string
}

type MoverOptions struct {
	ContinueOnErr  bool
	Force          bool
	TableToWriteTo string
	Operation      TableImportOp
}

type DataMoverOptions interface {
	WritesToTable() bool
	SrcName() string
	DestName() string
}

type DataMoverCloser interface {
	table.TableWriteCloser
	Flush(context.Context) (*doltdb.RootValue, error)
}

type DataWriter interface {
	WriteRows(ctx context.Context, inputChannel chan sql.Row, badRowCb func(*pipeline.TransformRowFailure) bool) error
	Commit(ctx context.Context) error
	Schema() sql.Schema
}

type DataMover struct {
	Rd         table.TableReadCloser
	Transforms *pipeline.TransformCollection
	Wr         table.TableWriteCloser
	ContOnErr  bool
}

type DataMoverCreationErrType string

const (
	CreateReaderErr   DataMoverCreationErrType = "Create reader error"
	NomsKindSchemaErr DataMoverCreationErrType = "Invalid schema error"
	SchemaErr         DataMoverCreationErrType = "Schema error"
	MappingErr        DataMoverCreationErrType = "Mapping error"
	ReplacingErr      DataMoverCreationErrType = "Replacing error"
	CreateMapperErr   DataMoverCreationErrType = "Mapper creation error"
	CreateWriterErr   DataMoverCreationErrType = "Create writer error"
	CreateSorterErr   DataMoverCreationErrType = "Create sorter error"
)

var ErrProvidedPkNotFound = errors.New("provided primary key not found")

type DataMoverCreationError struct {
	ErrType DataMoverCreationErrType
	Cause   error
}

func (dmce *DataMoverCreationError) String() string {
	return string(dmce.ErrType) + ": " + dmce.Cause.Error()
}

type GCTableWriteCloser interface {
	table.TableWriteCloser
	GC(ctx context.Context) error
}

// Move is the method that executes the pipeline which will move data from the pipeline's source DataLocation to it's
// dest DataLocation.  It returns the number of bad rows encountered during import, and an error.
func (imp *DataMover) Move(ctx context.Context, sch schema.Schema) (badRowCount int64, err error) {
	defer imp.Rd.Close(ctx)
	defer func() {
		closeErr := imp.Wr.Close(ctx)
		if err == nil {
			err = closeErr
		}

		if err == nil {
			if gcTWC, ok := imp.Wr.(GCTableWriteCloser); ok {
				err = gcTWC.GC(ctx)
			}
		}
	}()

	var badCount int64
	var rowErr error
	var printStarted bool
	var b bytes.Buffer
	badRowCB := func(trf *pipeline.TransformRowFailure) (quit bool) {
		if !imp.ContOnErr {
			rowErr = trf
			return true
		}

		if !printStarted {
			cli.PrintErrln("The following rows were skipped:")
			printStarted = true
		}

		r := pipeline.GetTransFailureRow(trf)

		if r != nil {
			err = writeBadRowToCli(ctx, r, sch, &b)
			if err != nil {
				return true
			}
		}

		atomic.AddInt64(&badCount, 1)
		return false
	}

	p := pipeline.NewAsyncPipeline(
		pipeline.ProcFuncForReader(ctx, imp.Rd),
		pipeline.ProcFuncForWriter(ctx, imp.Wr),
		imp.Transforms,
		badRowCB)
	p.Start()

	err = p.Wait()
	if err != nil {
		return 0, err
	}

	if rowErr != nil {
		return 0, rowErr
	}

	return badCount, nil
}

// writeBadRowToCli prints a bad row in a csv form to STDERR.
func writeBadRowToCli(ctx context.Context, r row.Row, sch schema.Schema, b *bytes.Buffer) error {
	sqlRow, err := sqlutil.DoltRowToSqlRow(r, sch)
	if err != nil {
		return err
	}

	wr := bufio.NewWriter(b)

	colValStrs := make([]*string, len(sqlRow))

	for colNum, col := range sqlRow {
		if col != nil {
			str := sqlutil.SqlColToStr(ctx, col)
			colValStrs[colNum] = &str
		} else {
			colValStrs[colNum] = nil
		}
	}

	err = csv.WriteCSVRow(wr, colValStrs, ",", false)
	if err != nil {
		return err
	}

	err = wr.Flush()
	if err != nil {
		return err
	}

	str := b.String()
	cli.PrintErr(str)

	return nil
}

func MoveDataToRoot(ctx context.Context, mover *DataMover, mvOpts DataMoverOptions, root *doltdb.RootValue, updateRoot func(c context.Context, r *doltdb.RootValue) error) (*doltdb.RootValue, int64, errhand.VerboseError) {
	var badCount int64
	var err error
	newRoot := &doltdb.RootValue{}

	badCount, err = mover.Move(ctx, mover.Wr.GetSchema())

	if err != nil {
		if pipeline.IsTransformFailure(err) {
			bdr := errhand.BuildDError("\nA bad row was encountered while moving data.")

			r := pipeline.GetTransFailureRow(err)
			if r != nil {
				bdr.AddDetails("Bad Row: " + row.Fmt(ctx, r, mover.Wr.GetSchema()))
			}

			details := pipeline.GetTransFailureDetails(err)

			bdr.AddDetails(details)
			bdr.AddDetails("These can be ignored using the '--continue'")

			return nil, badCount, bdr.Build()
		}
		return nil, badCount, errhand.BuildDError("An error occurred moving data:\n").AddCause(err).Build()
	}

	if mvOpts.WritesToTable() {
		wr := mover.Wr.(DataMoverCloser)
		newRoot, err = wr.Flush(ctx)
		if err != nil {
			return nil, badCount, errhand.BuildDError("Failed to apply changes to the table.").AddCause(err).Build()
		}

		rootHash, err := root.HashOf()
		if err != nil {
			return nil, badCount, errhand.BuildDError("Failed to hash the working value.").AddCause(err).Build()
		}

		newRootHash, err := newRoot.HashOf()
		if rootHash != newRootHash {
			err = updateRoot(ctx, newRoot)
			if err != nil {
				return nil, badCount, errhand.BuildDError("Failed to update the working value.").AddCause(err).Build()
			}
		}
	}

	return newRoot, badCount, nil
}

func MoveData(ctx context.Context, dEnv *env.DoltEnv, mover *DataMover, mvOpts DataMoverOptions) (int64, errhand.VerboseError) {
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return 0, errhand.BuildDError("Failed to fetch the working value.").AddCause(err).Build()
	}
	_, badCount, moveErr := MoveDataToRoot(ctx, mover, mvOpts, root, dEnv.UpdateWorkingRoot)
	if moveErr != nil {
		return badCount, moveErr
	}
	return badCount, nil
}

// SchAndTableNameFromFile reads a SQL schema file and creates a Dolt schema from it.
func SchAndTableNameFromFile(ctx context.Context, path string, fs filesys.ReadableFS) (string, sql.PrimaryKeySchema, error) {
	if path != "" {
		data, err := fs.ReadFile(path)

		if err != nil {
			return "", sql.PrimaryKeySchema{}, err
		}

		tn, sch, err := sqlutil.ParseCreateTableStatement(ctx, string(data))

		if err != nil {
			return "", sql.PrimaryKeySchema{}, fmt.Errorf("%s in schema file %s", err.Error(), path)
		}

		return tn, sch, nil
	} else {
		return "", sql.PrimaryKeySchema{}, errors.New("no schema file to parse")
	}
}

func InferSchema(ctx context.Context, rd table.TableReadCloser, tableName string, pks []string, args actions.InferenceArgs) (sql.PrimaryKeySchema, error) {
	outSch, err := actions.InferSchemaFromTableReader(ctx, rd, args)
	if err != nil {
		return sql.PrimaryKeySchema{}, err
	}

	for _, col := range outSch.Schema {
		col.Source = tableName
	}

	// Update the primary keys and source of the schema
	for _, pk := range pks {
		idx := outSch.IndexOf(pk, tableName)
		if idx < 0 {
			return sql.PrimaryKeySchema{}, ErrProvidedPkNotFound
		}

		outSch.Schema[idx].PrimaryKey = true
		outSch.PkOrdinals = append(outSch.PkOrdinals, idx)
	}

	err = sql.ValidateSchema(outSch.Schema)
	if err != nil {
		return sql.PrimaryKeySchema{}, errhand.BuildDError("invalid schema").AddCause(err).Build()
	}

	return outSch, nil
}

type TableImportOp string

const (
	CreateOp  TableImportOp = "overwrite"
	ReplaceOp TableImportOp = "replace"
	UpdateOp  TableImportOp = "update"
)
