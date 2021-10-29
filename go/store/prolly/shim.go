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

package prolly

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func NewEmptyMap(sch schema.Schema) Map {
	return Map{
		root:    emptyNode,
		keyDesc: keyDescriptorFromSchema(sch),
		valDesc: valueDescriptorFromSchema(sch),
	}
}

func ValueFromNode(nd Node) types.Value {
	return types.InlineBlob(nd)
}

func NodeFromValue(v types.Value) Node {
	return Node(v.(types.InlineBlob))
}

func ValueFromMap(m Map) types.Value {
	return types.InlineBlob(m.root)
}

func MapFromValue(v types.Value, sch schema.Schema, vrw types.ValueReadWriter) Map {
	return Map{
		root:    NodeFromValue(v),
		keyDesc: keyDescriptorFromSchema(sch),
		valDesc: valueDescriptorFromSchema(sch),
		nrw:     NewNodeStore(ChunkStoreFromVRW(vrw)),
	}
}

func ChunkStoreFromVRW(vrw types.ValueReadWriter) chunks.ChunkStore {
	switch x := vrw.(type) {
	case datas.Database:
		return datas.ChunkStoreFromDatabase(x)
	case *types.ValueStore:
		return x.ChunkStore()
	}
	panic("unknown ValueReadWriter")
}

func EmptyTreeChunkerFromMap(ctx context.Context, m Map) *TreeChunker {
	ch, err := newEmptyTreeChunker(ctx, m.nrw, newDefaultNodeSplitter)
	if err != nil {
		panic(err)
	}
	return ch
}

func keyDescriptorFromSchema(sch schema.Schema) val.TupleDesc {
	var tt []val.Type
	_ = sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		tt = append(tt, val.Type{
			Enc:      encodingFromNomsKind(col.Kind),
			Nullable: false,
		})
		return
	})
	return val.NewTupleDescriptor(tt...)
}

func valueDescriptorFromSchema(sch schema.Schema) (vd val.TupleDesc) {
	var tt []val.Type
	_ = sch.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		tt = append(tt, val.Type{
			Enc:      encodingFromNomsKind(col.Kind),
			Nullable: col.IsNullable(),
		})
		return
	})
	return val.NewTupleDescriptor(tt...)
}

func encodingFromNomsKind(k types.NomsKind) val.Encoding {
	switch k {
	case types.BoolKind:
		return val.Int8Enc
	case types.IntKind:
		return val.Int64Enc
	case types.UintKind:
		return val.Uint64Enc
	case types.FloatKind:
		return val.Float64Enc
	case types.StringKind:
		return val.StringEnc
	case types.BlobKind:
		return val.BytesEnc
	case types.InlineBlobKind:
		return val.BytesEnc
	default:
		panic("unknown nomds kind")
	}
}
