package dtables

import (
	"encoding/json"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
)

func newProllyCVTable(ctx *sql.Context, tblName string, root *doltdb.RootValue, rs RootSetter) (sql.Table, error) {
	tbl, tblName, ok, err := root.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, sql.ErrTableNotFound.New(tblName)
	}
	cvSch, err := tbl.GetConstraintViolationsSchema(ctx)
	if err != nil {
		return nil, err
	}
	sqlSch, err := sqlutil.FromDoltSchema(doltdb.DoltConstViolTablePrefix+tblName, cvSch)
	if err != nil {
		return nil, err
	}

	arts, err := tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	m := durable.ProllyMapFromArtifactIndex(arts)
	return &prollyConstraintViolationsTable{
		tblName: tblName,
		root:    root,
		sqlSch:  sqlSch,
		tbl:     tbl,
		rs:      rs,
		artM:    m,
	}, nil
}

// prollyConstraintViolationsTable is a sql.Table implementation that provides access to the constraint violations that exist
// for a user table for the v1 format.
type prollyConstraintViolationsTable struct {
	tblName string
	root    *doltdb.RootValue
	sqlSch  sql.PrimaryKeySchema
	tbl     *doltdb.Table
	rs      RootSetter
	artM    prolly.ArtifactMap
}

var _ sql.Table = (*prollyConstraintViolationsTable)(nil)
var _ sql.DeletableTable = (*prollyConstraintViolationsTable)(nil)

// Name implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) Name() string {
	return doltdb.DoltConstViolTablePrefix + cvt.tblName
}

// String implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) String() string {
	return doltdb.DoltConstViolTablePrefix + cvt.tblName
}

// Schema implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) Schema() sql.Schema {
	return cvt.sqlSch.Schema
}

// Partitions implements the interface sql.Table.
func (cvt *prollyConstraintViolationsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (cvt *prollyConstraintViolationsTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	idx, err := cvt.tbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	m := durable.ProllyMapFromArtifactIndex(idx)
	itr, err := m.IterAllCVs(ctx)
	if err != nil {
		return nil, err
	}
	sch, err := cvt.tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	kd, vd := shim.MapDescriptorsFromSchema(sch)
	return prollyCVIter{itr, sch, kd, vd}, nil
}

func (cvt *prollyConstraintViolationsTable) Deleter(context *sql.Context) sql.RowDeleter {
	ed := cvt.artM.Editor()
	p := cvt.artM.Pool()
	kd, _ := cvt.artM.Descriptors()
	kb := val.NewTupleBuilder(kd)

	return &prollyCVDeleter{
		kd:   kd,
		kb:   kb,
		ed:   ed,
		pool: p,
		cvt:  cvt,
	}
}

type prollyCVIter struct {
	itr    prolly.ArtifactIter
	sch    schema.Schema
	kd, vd val.TupleDesc
}

func (itr prollyCVIter) Next(ctx *sql.Context) (sql.Row, error) {
	art, err := itr.itr.Next(ctx)
	if err != nil {
		return nil, err
	}

	r := make(sql.Row, itr.sch.GetAllCols().Size()+3)
	r[0] = hash.Of(art.HeadCmHash).String()
	r[1] = mapCVType(art.ArtType)

	var meta prolly.ConstraintViolationMeta
	err = json.Unmarshal(art.Metadata, &meta)
	if err != nil {
		return nil, err
	}

	o := 2
	for i := 0; i < itr.kd.Count(); i++ {
		r[o+i], err = index.GetField(itr.kd, i, art.Key)
		if err != nil {
			return nil, err
		}
	}
	o += itr.kd.Count()

	for i := 0; i < itr.vd.Count(); i++ {
		r[o+i], err = index.GetField(itr.vd, i, meta.Value)
		if err != nil {
			return nil, err
		}
	}
	o += itr.vd.Count()

	var m merge.FkCVMeta
	err = json.Unmarshal(meta.VInfo, &m)
	if err != nil {
		return nil, err
	}
	r[o] = m

	return r, nil
}

type prollyCVDeleter struct {
	kd   val.TupleDesc
	kb   *val.TupleBuilder
	pool pool.BuffPool
	ed   prolly.ArtifactsEditor
	cvt  *prollyConstraintViolationsTable
}

var _ sql.RowDeleter = (*prollyCVDeleter)(nil)

// Delete implements the interface sql.RowDeleter.
func (d *prollyCVDeleter) Delete(ctx *sql.Context, r sql.Row) error {
	// first part of the artifact key is the keys of the source table
	for i := 0; i < d.kd.Count()-2; i++ {
		err := index.PutField(d.kb, i, r[i+2])
		if err != nil {
			return err
		}
	}

	// then the hash
	//h := hash.Parse(r[0].(string))
	h := hash.Of(nil)
	d.kb.PutAddress(d.kd.Count()-2, h[:])

	// Finally the artifact type
	artType := unmapCVType(r[1].(string))
	d.kb.PutUint8(d.kd.Count()-1, uint8(artType))

	key := d.kb.Build(d.pool)
	err := d.ed.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (d *prollyCVDeleter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (d *prollyCVDeleter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (d *prollyCVDeleter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close implements the interface sql.RowDeleter.
func (d *prollyCVDeleter) Close(ctx *sql.Context) error {
	arts, err := d.ed.Flush(ctx)
	if err != nil {
		return err
	}

	// TODO: We can delete from more than one table in a single statement. Root
	// updates should be restricted to write session and not individual table
	// editors.

	updatedTbl, err := d.cvt.tbl.SetArtifacts(ctx, durable.ArtifactIndexFromProllyMap(arts))
	if err != nil {
		return err
	}

	updatedRoot, err := d.cvt.root.PutTable(ctx, d.cvt.tblName, updatedTbl)
	if err != nil {
		return err
	}

	return d.cvt.rs.SetRoot(ctx, updatedRoot)
}

func mapCVType(artifactType prolly.ArtifactType) (outType string) {
	switch artifactType {
	case prolly.ArtifactTypeForeignKeyViol:
		outType = "foreign key"
	case prolly.ArtifactTypeUniqueKeyViol:
		outType = "unique key"
	case prolly.ArtifactTypeChkConsViol:
		outType = "check constraint"
	default:
		panic("unhandled cv type")
	}
	return
}

func unmapCVType(in string) (out prolly.ArtifactType) {
	switch in {
	case "foreign key":
		out = prolly.ArtifactTypeForeignKeyViol
	case "unique key":
		out = prolly.ArtifactTypeUniqueKeyViol
	case "check constraint":
		out = prolly.ArtifactTypeChkConsViol
	default:
		panic("unhandled cv type")
	}
	return
}

func (itr prollyCVIter) Close(ctx *sql.Context) error {
	return nil
}
