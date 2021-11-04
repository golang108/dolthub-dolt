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
	"encoding/binary"
	"math"

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	cumulativeCountSize = val.ByteSize(6)
	nodeCountSize       = val.ByteSize(2)
	treeLevelSize       = val.ByteSize(1)

	maxNodeDataSize = uint64(math.MaxUint16)
)

type nodeItem []byte

func (i nodeItem) size() val.ByteSize {
	return val.ByteSize(len(i))
}

type nodePair [2]nodeItem

func (p nodePair) key() nodeItem {
	return p[0]
}

func (p nodePair) value() nodeItem {
	return p[1]
}

type Node []byte

func makeProllyNode(pool pool.BuffPool, level uint64, items ...nodeItem) (nd Node) {
	var sz uint64
	for _, item := range items {
		sz += uint64(item.size())

	}
	count := len(items)

	if sz > maxNodeDataSize {
		panic("items exceeded max chunk size")
	}

	pos := val.ByteSize(sz)
	pos += val.OffsetsSize(count)
	pos += cumulativeCountSize
	pos += nodeCountSize
	pos += treeLevelSize

	nd = pool.Get(uint64(pos))

	c := cumulativeCountFromItems(level, items)
	writeCumulativeCount(nd, c)
	writeItemCount(nd, count)
	writeTreeLevel(nd, level)

	pos = 0
	offs, _ := nd.offsets()
	for i, item := range items {
		copy(nd[pos:pos+item.size()], item)
		offs.Put(i, pos)
		pos += item.size()
	}

	return nd
}

func cumulativeCountFromItems(level uint64, items []nodeItem) (c uint64) {
	if level == 0 {
		return uint64(len(items))
	}

	for i := 1; i < len(items); i += 2 {
		c += metaValue(items[i]).GetCumulativeCount()
	}
	return c
}

func (nd Node) getItem(i int) nodeItem {
	offs, itemStop := nd.offsets()
	start, stop := offs.GetBounds(i, itemStop)
	return nodeItem(nd[start:stop])
}

func (nd Node) getPair(i int) (p nodePair) {
	offs, itemStop := nd.offsets()
	start, stop := offs.GetBounds(i, itemStop)
	p[0] = nodeItem(nd[start:stop])
	start, stop = offs.GetBounds(i+1, itemStop)
	p[1] = nodeItem(nd[start:stop])
	return
}

func (nd Node) size() val.ByteSize {
	return val.ByteSize(len(nd))
}

func (nd Node) level() int {
	return int(nd[nd.size()-treeLevelSize])
}

func (nd Node) nodeCount() int {
	stop := nd.size() - treeLevelSize
	start := stop - nodeCountSize
	return int(binary.LittleEndian.Uint16(nd[start:stop]))
}

func (nd Node) cumulativeCount() uint64 {
	stop := nd.size() - treeLevelSize - nodeCountSize
	start := stop - cumulativeCountSize
	buf := nd[start:stop]
	return readUint48(buf)
}

func (nd Node) offsets() (offs val.Offsets, itemStop val.ByteSize) {
	stop := nd.size() - treeLevelSize - nodeCountSize - cumulativeCountSize
	itemStop = stop - val.OffsetsSize(nd.nodeCount())
	return val.Offsets(nd[itemStop:stop]), itemStop
}

func (nd Node) leafNode() bool {
	return nd.level() == 0
}

func (nd Node) empty() bool {
	return len(nd) == 0 || nd.nodeCount() == 0
}

func writeTreeLevel(nd Node, level uint64) {
	nd[nd.size()-treeLevelSize] = uint8(level)
}

func writeItemCount(nd Node, count int) {
	stop := nd.size() - treeLevelSize
	start := stop - nodeCountSize
	binary.LittleEndian.PutUint16(nd[start:stop], uint16(count))
}

func writeCumulativeCount(nd Node, count uint64) {
	stop := nd.size() - treeLevelSize - nodeCountSize
	start := stop - cumulativeCountSize
	writeUint48(nd[start:stop], count)
}

const (
	uint48Size = 6
	uint48Max  = uint64(1<<48 - 1)
)

func writeUint48(dest []byte, u uint64) {
	if len(dest) != uint48Size {
		panic("incorrect number of bytes for uint48")
	}
	if u > uint48Max {
		panic("uint is greater than max uint")
	}

	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], u)
	copy(dest, tmp[:uint48Size])
}

func readUint48(src []byte) (u uint64) {
	if len(src) != uint48Size {
		panic("incorrect number of bytes for uint48")
	}
	var tmp [8]byte
	copy(tmp[:uint48Size], src)
	u = binary.LittleEndian.Uint64(tmp[:])
	return
}
