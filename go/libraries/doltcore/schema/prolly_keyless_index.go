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

package schema

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

// prollyKeylessIndex is an index used by the prolly format, specifically for indexes on keyless tables. Such indexes
// have an additional column that contains the hash of the parent table. This hash column is not found in the parent
// table, and therefore cannot be referenced through normal means. This then functions as a wrapper for such
// indexes, so that all returned values will include the additional column.
type prollyKeylessIndex struct {
	base indexImpl
}

const prollyKeylessColName = "__dolt_prolly_keyless_row_hash"

var prollyKeylessCol = Column{
	Name:        prollyKeylessColName,
	Tag:         KeylessRowIdTag,
	Kind:        types.UUIDKind,
	IsPartOfPK:  true,
	TypeInfo:    typeinfo.FromKind(types.UUIDKind),
	Constraints: nil,
}

var _ Index = (*prollyKeylessIndex)(nil)

// AllTags implements the interface Index.
func (p *prollyKeylessIndex) AllTags() []uint64 {
	allTags := make([]uint64, len(p.base.allTags)+1)
	copy(allTags, p.base.allTags)
	allTags[len(allTags)-1] = KeylessRowIdTag
	return allTags
}

// ColumnNames implements the interface Index.
func (p *prollyKeylessIndex) ColumnNames() []string {
	return append(p.base.ColumnNames(), prollyKeylessColName)
}

// Comment implements the interface Index.
func (p *prollyKeylessIndex) Comment() string {
	return p.base.Comment()
}

// Count implements the interface Index.
func (p *prollyKeylessIndex) Count() int {
	return p.base.Count()
}

// DeepEquals implements the interface Index.
func (p *prollyKeylessIndex) DeepEquals(other Index) bool {
	return p.base.DeepEquals(other)
}

// Equals implements the interface Index.
func (p *prollyKeylessIndex) Equals(other Index) bool {
	return p.base.Equals(other)
}

// GetColumn implements the interface Index.
func (p *prollyKeylessIndex) GetColumn(tag uint64) (Column, bool) {
	if tag == KeylessRowIdTag {
		return prollyKeylessCol, true
	}
	return p.base.GetColumn(tag)
}

// IndexedColumnTags implements the interface Index.
func (p *prollyKeylessIndex) IndexedColumnTags() []uint64 {
	return p.base.IndexedColumnTags()
}

// IsUnique implements the interface Index.
func (p *prollyKeylessIndex) IsUnique() bool {
	return p.base.IsUnique()
}

// IsUserDefined implements the interface Index.
func (p *prollyKeylessIndex) IsUserDefined() bool {
	return p.base.IsUserDefined()
}

// KeylessParent implements the interface Index.
func (p *prollyKeylessIndex) KeylessParent() bool {
	return true
}

// Name implements the interface Index.
func (p *prollyKeylessIndex) Name() string {
	return p.base.Name()
}

// PrimaryKeyTags implements the interface Index.
func (p *prollyKeylessIndex) PrimaryKeyTags() []uint64 {
	return []uint64{KeylessRowIdTag}
}

// Schema implements the interface Index.
func (p *prollyKeylessIndex) Schema() Schema {
	cols := make([]Column, len(p.base.allTags)+1)
	for i, tag := range p.base.allTags {
		col := p.base.indexColl.colColl.TagToCol[tag]
		cols[i] = Column{
			Name:        col.Name,
			Tag:         tag,
			Kind:        col.Kind,
			IsPartOfPK:  true,
			TypeInfo:    col.TypeInfo,
			Constraints: nil,
		}
	}
	cols[len(cols)-1] = prollyKeylessCol
	allCols := NewColCollection(cols...)
	nonPkCols := NewColCollection()
	return &schemaImpl{
		pkCols:          allCols,
		nonPKCols:       nonPkCols,
		allCols:         allCols,
		indexCollection: NewIndexCollection(nil, nil),
		checkCollection: NewCheckCollection(),
	}
}

// ToTableTuple implements the interface Index.
func (p *prollyKeylessIndex) ToTableTuple(ctx context.Context, fullKey types.Tuple, format *types.NomsBinFormat) (types.Tuple, error) {
	return p.base.ToTableTuple(ctx, fullKey, format)
}

// VerifyMap implements the interface Index.
func (p *prollyKeylessIndex) VerifyMap(ctx context.Context, iter types.MapIterator, nbf *types.NomsBinFormat) error {
	return p.base.VerifyMap(ctx, iter, nbf)
}

// change implements the interface Index.
func (p *prollyKeylessIndex) change(tags []uint64, ixc *indexCollectionImpl) {
	p.base.change(tags, ixc)
}

// copy implements the interface Index.
func (p *prollyKeylessIndex) copy() Index {
	return &prollyKeylessIndex{*p.base.copy().(*indexImpl)}
}

// renameIndex implements the interface Index.
func (p *prollyKeylessIndex) renameIndex(newName string) {
	p.base.renameIndex(newName)
}
