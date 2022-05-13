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

package val

import (
	"math"

	"github.com/dolthub/dolt/go/store/pool"
)

const (
	MaxTupleFields            = 4096
	MaxTupleDataSize ByteSize = math.MaxUint16

	countSize ByteSize = 2
)

// Tuple is a collection of values encoded as a byte string.
//
//   +---------+---------+-----+---------+---------+-------------+
//   | Value 0 | Value 1 | ... | Value K | Offsets | Field Count |
//   +---------+---------+-----+---------+---------+-------------+
//
//   Tuples are null-compacted and allow random-access via offsets. Field values
//   are packed contiguously from the front of the Tuple's byte string. Offsets
//   and a field count follow the field value array. The offset of the first
//   field is omitted from the offsets array. A Tuple with N fields stores N-1
//   offsets in its suffix. Offsets are stored for all fields, regardless of
//   whether a field's value is null. Null fields are encoded as zero length
//   values. As a consequence, encodings must differentiate null-values and
//   zero-length values (by null-terminating for example).
//   Tuples read and write Values as byte slices. (De)serialization is delegated
//   to Tuple Descriptors, which know a Tuple's schema and associated encodings.
//
type Tuple []byte

var EmptyTuple = Tuple([]byte{0, 0})

func NewTuple(pool pool.BuffPool, values ...[]byte) Tuple {
	var count int
	var pos ByteSize
	for _, v := range values {
		if isNull(v) {
			continue
		}
		count++
		pos += sizeOf(v)
	}
	if len(values) > MaxTupleFields {
		panic("tuple field maxIdx exceeds maximum")
	}
	if pos > MaxTupleDataSize {
		panic("tuple data size exceeds maximum")
	}

	tup, offs := allocateTuple(pool, pos, len(values))

	count = 0
	pos = ByteSize(0)
	for _, v := range values {
		writeOffset(count, pos, offs)
		count++

		if isNull(v) {
			continue
		}

		copy(tup[pos:pos+sizeOf(v)], v)
		pos += sizeOf(v)
	}

	return tup
}

func CloneTuple(pool pool.BuffPool, tup Tuple) Tuple {
	buf := pool.Get(uint64(len(tup)))
	copy(buf, tup)
	return buf
}

func allocateTuple(pool pool.BuffPool, bufSz ByteSize, fields int) (tup Tuple, offs offsets) {
	offSz := offsetsSize(fields)
	tup = pool.Get(uint64(bufSz + offSz + countSize))

	writeFieldCount(tup, fields)
	offs = offsets(tup[bufSz : bufSz+offSz])

	return
}

// GetField returns the value for field |i|.
func (tup Tuple) GetField(i int) []byte {
	cnt := tup.Count()
	if i >= cnt {
		return nil
	}

	sz := ByteSize(len(tup))
	split := sz - uint16Size*ByteSize(cnt)
	offs := tup[split : sz-countSize]

	start, stop := uint16(0), uint16(split)
	if i*2 < len(offs) {
		pos := i * 2
		stop = readUint16(offs[pos : pos+2])
	}
	if i > 0 {
		pos := (i - 1) * 2
		start = readUint16(offs[pos : pos+2])
	}

	if start == stop {
		return nil // NULL
	}

	return tup[start:stop]
}

// GetManyFields takes a sorted slice of ordinals |indexes| and returns the requested
// tuple fields. It populates field data into |slices| to avoid allocating.
func (tup Tuple) GetManyFields(indexes []int, slices [][]byte) [][]byte {
	return sliceManyFields(tup, indexes, slices)
}

func (tup Tuple) FieldIsNull(i int) bool {
	return tup.GetField(i) == nil
}

func (tup Tuple) Count() int {
	sl := tup[len(tup)-int(countSize):]
	return int(readUint16(sl))
}

func isNull(val []byte) bool {
	return val == nil
}

func sizeOf(val []byte) ByteSize {
	return ByteSize(len(val))
}

func writeFieldCount(tup Tuple, count int) {
	sl := tup[len(tup)-int(countSize):]
	writeUint16(sl, uint16(count))
}

func sliceManyFields(tuple Tuple, indexes []int, slices [][]byte) [][]byte {
	cnt := tuple.Count()
	sz := ByteSize(len(tuple))
	split := sz - uint16Size*ByteSize(cnt)

	data := tuple[:split]
	offs := offsets(tuple[split : sz-countSize])

	// if count is 1, we assume |indexes| is [0]
	if cnt == 1 {
		slices[0] = data
		if len(data) == 0 {
			slices[0] = nil
		}
		return slices
	}

	subset := slices
	// we don't have a "stop" offset for the last field
	n := len(slices)
	if indexes[n-1] == cnt-1 {
		o := readUint16(offs[len(offs)-2:])
		slices[n-1] = data[o:]
		indexes = indexes[:n-1]
		subset = subset[:n-1]
	}

	// we don't have a "start" offset for the first field
	if len(indexes) > 0 && indexes[0] == 0 {
		o := readUint16(offs[:2])
		slices[0] = data[:o]
		indexes = indexes[1:]
		subset = subset[1:]
	}

	for i, k := range indexes {
		start := readUint16(offs[(k-1)*2 : k*2])
		stop := readUint16(offs[k*2 : (k+1)*2])
		subset[i] = tuple[start:stop]
	}

	for i := range slices {
		if len(slices[i]) == 0 {
			slices[i] = nil
		}
	}

	return slices
}
