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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/golang/snappy"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	journalFile = "nbs_journal"
	journalSize = 256 * 1024 * 1024
)

type recordKind uint8

const (
	rootHashKind recordKind = 1
	chunkKind    recordKind = 2
)

type journalRecord struct {
	kind     recordKind
	address  addr
	cmpData  []byte
	checksum uint32
}

func readJournalRecord(buf []byte) (rec journalRecord) {
	rec.kind = recordKind(buf[0])
	buf = buf[1:]
	copy(rec.address[:], buf)
	buf = buf[addrSize:]
	rec.cmpData = buf[:len(buf)-checksumSize]
	tail := buf[len(buf)-checksumSize:]
	rec.checksum = binary.LittleEndian.Uint32(tail)
	return
}

func writeJournalRecord(buf []byte, kind recordKind, a addr, data []byte) (uint32, []byte) {
	var n uint32
	buf[n] = byte(kind)
	n += 1
	copy(buf[n:], a[:])
	n += addrSize
	compressed := snappy.Encode(buf[n:], data) // todo: zstd
	n += uint32(len(compressed))
	binary.BigEndian.PutUint32(buf[n:], crc(compressed))
	n += checksumSize
	return n, buf[n:]
}

type chunkReaderAppender interface {
	chunkReader
	// append attempts to add a memTable in an existing chunkSource, returning true on success.
	append(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (bool, error)
}

type chunkJournal struct {
	journal  *os.File
	entries  map[addr]journalEntry
	manifest manifestContents
	mu       sync.RWMutex

	offset         int64
	uncompressedSz uint64
}

var _ chunkReaderAppender = &chunkJournal{}
var _ manifestUpdater = &chunkJournal{}

type journalEntry struct {
	offset int64
	length uint32
}

func newJournalTableSet(ctx context.Context, dir string, tables tableSet) (tableSet, error) {
	panic("unimplemented")
}

// append implements chunkReaderAppender
func (cj *chunkJournal) append(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (bool, error) {
	// synchronously flush |mt|
	cj.mu.Lock()
	defer cj.mu.Unlock()

	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		if _, err := haver.hasMany(mt.order); err != nil {
			return false, err
		}
		sort.Sort(hasRecordByOrder(mt.order)) // restore "insertion" order for write
	}

	// todo: allocate based on absent novel chunks
	buf := make([]byte, maxTableSize(uint64(len(mt.order)), mt.totalData))

	for _, record := range mt.order {
		if !record.has {
			var n uint32
			n, buf = writeJournalRecord(buf, chunkKind, *record.a, mt.chunks[*record.a])
			cj.entries[*record.a] = journalEntry{
				offset: cj.offset,
				length: n,
			}
			cj.offset += int64(n)
			cj.uncompressedSz += uint64(n)
		}
	}

	n, err := cj.journal.Write(buf)
	if err != nil {
		return false, err
	} else if n < len(buf) {
		return false, fmt.Errorf("incomplete write (%d < %d)", n, len(buf))
	}

	return true, nil
}

func (cj *chunkJournal) Name() string {
	return journalFile
}

func (cj *chunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (bool, manifestContents, error) {
	cj.mu.Lock()
	defer cj.mu.Unlock()
	if err := readHook(); err != nil {
		return false, manifestContents{}, err
	}
	return true, cj.manifest, nil
}

func (cj *chunkJournal) Update(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	cj.mu.Lock()
	defer cj.mu.Unlock()
	if err := writeHook(); err != nil {
		return manifestContents{}, err
	}

	curr := cj.manifest
	if curr.lock != lastLock {
		return curr, nil // stale
	}
	if curr.manifestVers != next.manifestVers ||
		curr.nbfVers != next.nbfVers ||
		curr.gcGen != next.gcGen {
		panic("manifest metadata does not match")
	}
	cj.manifest = next
	return cj.manifest, nil
}

func (cj *chunkJournal) Close() error {
	// chunkJournal does not own |cs| or |mt|. No need to close them.
	return cj.journal.Close()
}

func (cj *chunkJournal) has(h addr) (bool, error) {
	cj.mu.RLock()
	defer cj.mu.RUnlock()
	_, ok := cj.entries[h]
	return ok, nil
}

func (cj *chunkJournal) hasMany(addrs []hasRecord) (missing bool, err error) {
	cj.mu.RLock()
	defer cj.mu.RUnlock()
	for i := range addrs {
		a := addrs[i].a
		if _, ok := cj.entries[*a]; ok {
			addrs[i].has = true
		} else {
			missing = true
		}
	}
	return
}

func (cj *chunkJournal) getCompressed(ctx context.Context, h addr, stats *Stats) (CompressedChunk, error) {
	cj.mu.RLock()
	defer cj.mu.RUnlock()
	e, ok := cj.entries[h]
	if !ok {
		return CompressedChunk{}, nil
	}
	b := make([]byte, e.length)

	// todo: validate |n|?
	_, err := cj.journal.ReadAt(b, e.offset)
	if err != nil {
		return CompressedChunk{}, err
	}
	rec := readJournalRecord(b)

	return NewCompressedChunk(hash.Hash(h), rec.cmpData)
}

func (cj *chunkJournal) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	cc, err := cj.getCompressed(ctx, h, stats)
	if err != nil {
		return nil, err
	} else if cc.IsEmpty() {
		return nil, nil
	}

	ch, err := cc.ToChunk()
	if err != nil {
		return nil, err
	}
	return ch.Data(), nil
}

func (cj *chunkJournal) getMany(ctx context.Context, _ *errgroup.Group, reqs []getRecord, found func(context.Context, *chunks.Chunk), stats *Stats) (bool, error) {
	var remaining bool
	// todo: read planning
	for i := range reqs {
		data, err := cj.get(ctx, *reqs[i].a, stats)
		if err != nil {
			return false, err
		} else if data != nil {
			ch := chunks.NewChunkWithHash(hash.Hash(*reqs[i].a), data)
			found(ctx, &ch)
		} else {
			remaining = true
		}
	}
	return remaining, nil
}

func (cj *chunkJournal) getManyCompressed(ctx context.Context, _ *errgroup.Group, reqs []getRecord, found func(context.Context, CompressedChunk), stats *Stats) (bool, error) {
	var remaining bool
	// todo: read planning
	for i := range reqs {
		cc, err := cj.getCompressed(ctx, *reqs[i].a, stats)
		if err != nil {
			return false, err
		} else if cc.IsEmpty() {
			remaining = true
		} else {
			found(ctx, cc)
		}
	}
	return remaining, nil
}

func (cj *chunkJournal) extract(ctx context.Context, chunks chan<- extractRecord) error {
	panic("unimplemented")
}

func (cj *chunkJournal) count() (uint32, error) {
	return uint32(len(cj.entries)), nil
}

func (cj *chunkJournal) uncompressedLen() (uint64, error) {
	return cj.uncompressedSz, nil
}

func (cj *chunkJournal) hash() (addr, error) {
	return addr{}, nil
}
