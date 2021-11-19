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

package sqle

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/lookup"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type IndexLookupKeyIterator interface {
	// NextKey returns the next key if it exists, and io.EOF if it does not.
	NextKey(ctx *sql.Context) (row.TaggedValues, error)
}

type doltIndexLookup struct {
	idx    DoltIndex
	ranges []lookup.Range // The collection of ranges that represent this lookup.
}

// nomsRangeCheck is used to compare a tuple against a set of comparisons in the noms row iterator.
type nomsRangeCheck []columnBounds

var _ noms.InRangeCheck = nomsRangeCheck{}

func (il *doltIndexLookup) String() string {
	// TODO: this could be expanded with additional info (like the expression used to create the index lookup)
	return fmt.Sprintf("doltIndexLookup:%s", il.idx.ID())
}

func (il *doltIndexLookup) IndexRowData() prolly.Map {
	return il.idx.IndexRowData()
}

// Index implements the interface sql.IndexLookup
func (il *doltIndexLookup) Index() sql.Index {
	return il.idx
}

// Intersection implements sql.MergeableIndexLookup
func (il *doltIndexLookup) Intersection(indexLookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	rangeCombinations := make([][]lookup.Range, len(il.ranges))
	for i, ilRange := range il.ranges {
		rangeCombinations[i] = []lookup.Range{ilRange}
	}
	for _, indexLookup := range indexLookups {
		otherIl, ok := indexLookup.(*doltIndexLookup)
		if !ok {
			return nil, fmt.Errorf("failed to intersect sql.IndexLookup with type '%T'", indexLookup)
		}
		var newRangeCombination [][]lookup.Range
		for _, rangeCombination := range rangeCombinations {
			for _, ilRange := range otherIl.ranges {
				rc := make([]lookup.Range, len(rangeCombination)+1)
				copy(rc, rangeCombination)
				rc[len(rangeCombination)] = ilRange
				newRangeCombination = append(newRangeCombination, rc)
			}
		}
		rangeCombinations = newRangeCombination
	}
	var newRanges []lookup.Range
	var err error
	var ok bool
	for _, rangeCombination := range rangeCombinations {
		intersectedRange := lookup.AllRange()
		for _, rangeToIntersect := range rangeCombination {
			intersectedRange, ok, err = intersectedRange.TryIntersect(rangeToIntersect)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
		}
		if !intersectedRange.IsEmpty() {
			newRanges = append(newRanges, intersectedRange)
		}
	}
	newRanges, err = lookup.SimplifyRanges(newRanges)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx:    il.idx,
		ranges: newRanges,
	}, nil
}

// Union implements sql.MergeableIndexLookup
func (il *doltIndexLookup) Union(indexLookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	var ranges []lookup.Range
	var err error
	if len(il.ranges) == 0 {
		ranges = []lookup.Range{lookup.EmptyRange()}
	} else {
		ranges = make([]lookup.Range, len(il.ranges))
		copy(ranges, il.ranges)
	}
	for _, indexLookup := range indexLookups {
		otherIl, ok := indexLookup.(*doltIndexLookup)
		if !ok {
			return nil, fmt.Errorf("failed to union sql.IndexLookup with type '%T'", indexLookup)
		}
		ranges = append(ranges, otherIl.ranges...)
	}
	ranges, err = lookup.SimplifyRanges(ranges)
	if err != nil {
		return nil, err
	}
	return &doltIndexLookup{
		idx:    il.idx,
		ranges: ranges,
	}, nil
}

// RowIter returns a row iterator for this index lookup. The iterator will return the single matching row for the index.
func (il *doltIndexLookup) RowIter(ctx *sql.Context, rowData prolly.Map, columns []string) (sql.RowIter, error) {
	return il.RowIterForRanges(ctx, rowData, columns, il.ranges)
}

func (il *doltIndexLookup) indexCoversCols(cols []string) bool {
	if cols == nil {
		return false
	}

	idxCols := il.idx.IndexSchema().GetPKCols()
	covers := true
	for _, colName := range cols {
		if _, ok := idxCols.GetByNameCaseInsensitive(colName); !ok {
			covers = false
			break
		}
	}

	return covers
}

func (il *doltIndexLookup) RowIterForRanges(ctx *sql.Context, rowData types.Map, ranges []lookup.Range, columns []string) (sql.RowIter, error) {
	readRanges := make([]*noms.ReadRange, len(ranges))
	for i, lookupRange := range ranges {
		readRanges[i] = lookupRange.ToReadRange()
	}

	nrr := noms.NewNomsRangeReader(il.idx.IndexSchema(), rowData, readRanges)

	covers := il.indexCoversCols(columns)
	if covers {
		return NewCoveringIndexRowIterAdapter(ctx, il.idx, nrr, columns), nil
	} else {
		return NewIndexLookupRowIterAdapter(ctx, il.idx, nrr), nil
	}
	sch := il.idx.Schema()

	return rowIterFromMapIter(ctx, sch, projs, rows, iter)
}

// Between returns whether the given types.Value is between the bounds. In addition, this returns if the value is outside
// the bounds and above the upperbound.
func (cb columnBounds) Between(ctx context.Context, nbf *types.NomsBinFormat, val types.Value) (ok bool, over bool, err error) {
	switch cb.boundsCase {
	case boundsCase_infinity_infinity:
		return true, false, nil
	case boundsCase_infinity_lessEquals:
		ok, err := cb.upperbound.Less(nbf, val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_infinity_less:
		ok, err := val.Less(nbf, cb.upperbound)
		if err != nil || !ok {
			return false, true, err
		}
	case boundsCase_greaterEquals_infinity:
		ok, err := val.Less(nbf, cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
	case boundsCase_greaterEquals_lessEquals:
		ok, err := val.Less(nbf, cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
		ok, err = cb.upperbound.Less(nbf, val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_greaterEquals_less:
		ok, err := val.Less(nbf, cb.lowerbound)
		if err != nil || ok {
			return false, false, err
		}
		ok, err = val.Less(nbf, cb.upperbound)
		if err != nil || !ok {
			return false, true, err
		}
	case boundsCase_greater_infinity:
		ok, err := cb.lowerbound.Less(nbf, val)
		if err != nil || !ok {
			return false, false, err
		}
	case boundsCase_greater_lessEquals:
		ok, err := cb.lowerbound.Less(nbf, val)
		if err != nil || !ok {
			return false, false, err
		}
		ok, err = cb.upperbound.Less(nbf, val)
		if err != nil || ok {
			return false, true, err
		}
	case boundsCase_greater_less:
		ok, err := cb.lowerbound.Less(nbf, val)
		if err != nil || !ok {
			return false, false, err
		}
		ok, err = val.Less(nbf, cb.upperbound)
		if err != nil || !ok {
			return false, true, err
		}
	default:
		return false, false, fmt.Errorf("unknown bounds")
	}
	return true, false, nil
}

// Equals returns whether the calling columnBounds is equivalent to the given columnBounds.
func (cb columnBounds) Equals(otherBounds columnBounds) bool {
	if cb.boundsCase != otherBounds.boundsCase {
		return false
	}
	if cb.lowerbound == nil || otherBounds.lowerbound == nil {
		if cb.lowerbound != nil || otherBounds.lowerbound != nil {
			return false
		}
	} else if !cb.lowerbound.Equals(otherBounds.lowerbound) {
		return false
	}
	if cb.upperbound == nil || otherBounds.upperbound == nil {
		if cb.upperbound != nil || otherBounds.upperbound != nil {
			return false
		}
	} else if !cb.upperbound.Equals(otherBounds.upperbound) {
		return false
	}
	return true
}

// Check implements the interface noms.InRangeCheck.
func (nrc nomsRangeCheck) Check(ctx context.Context, tuple types.Tuple) (valid bool, skip bool, err error) {
	itr := types.TupleItrPool.Get().(*types.TupleIterator)
	defer types.TupleItrPool.Put(itr)
	err = itr.InitForTuple(tuple)
	if err != nil {
		return false, false, err
	}
	nbf := tuple.Format()

	for i := 0; i < len(nrc) && itr.HasMore(); i++ {
		if err := itr.Skip(); err != nil {
			return false, false, err
		}
		_, val, err := itr.Next()
		if err != nil {
			return false, false, err
		}
		if val == nil {
			break
		}

		ok, over, err := nrc[i].Between(ctx, nbf, val)
		if err != nil {
			return false, false, err
		}
		if !ok {
			return i != 0 || !over, true, nil
		}
	}
	return true, false, nil
}

// Equals returns whether the calling nomsRangeCheck is equivalent to the given nomsRangeCheck.
func (nrc nomsRangeCheck) Equals(otherNrc nomsRangeCheck) bool {
	if len(nrc) != len(otherNrc) {
		return false
	}
	for i := range nrc {
		if !nrc[i].Equals(otherNrc[i]) {
			return false
		}
	}
	return true
}

type keyIter interface {
	ReadKey(ctx context.Context) (val.Tuple, error)
}
