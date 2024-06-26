// Copyright 2024 Dolthub, Inc.
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

package nbs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/gozstd"
	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"
)

const defaultDictionarySize = 1 << 12 // NM4 - maybe just select the largest chunk. TBD.
const maxSamples = 1000
const minSamples = 25

func BuildArchive(ctx context.Context, cs chunks.ChunkStore, dagGroups *ChunkRelations, progress chan interface{}) (err error) {
	if gs, ok := cs.(*GenerationalNBS); ok {
		outPath, _ := gs.oldGen.Path()
		oldgen := gs.oldGen.tables.upstream

		swapMap := make(map[hash.Hash]hash.Hash)

		for tf, ogcs := range oldgen {
			// NM4 - We should probably provide a way to pick a particular table file to build an archive for.

			idx, err := ogcs.index()
			if err != nil {
				return err
			}

			archivePath := ""
			archiveName := hash.Hash{}
			archivePath, archiveName, err = convertTableFileToArchive(ctx, ogcs, idx, dagGroups, outPath, progress)
			if err != nil {
				return err
			}

			err = verifyAllChunks(idx, archivePath)
			if err != nil {
				return err
			}

			swapMap[tf] = archiveName
		}

		//NM4 TODO: This code path must only be run on an offline database. We should add a check for that.
		specs, err := gs.oldGen.tables.toSpecs()
		newSpecs := make([]tableSpec, 0, len(specs))
		for _, spec := range specs {
			if newSpec, exists := swapMap[spec.name]; exists {
				newSpecs = append(newSpecs, tableSpec{newSpec, spec.chunkCount})
			} else {
				newSpecs = append(newSpecs, spec)
			}
		}
		err = gs.oldGen.swapTables(ctx, newSpecs)
		if err != nil {
			return err
		}
	} else {
		return errors.New("Modern DB Expected")
	}
	return nil
}

func convertTableFileToArchive(ctx context.Context, cs chunkSource, idx tableIndex, dagGroups *ChunkRelations, archivePath string, progress chan interface{}) (string, hash.Hash, error) {
	allChunks, defaultSamples, err := gatherAllChunks(ctx, cs, idx)
	if err != nil {
		return "", hash.Hash{}, err
	}

	var defaultDict []byte
	if len(defaultSamples) >= minSamples {
		defaultDict = buildDictionary(defaultSamples)
	} else {
		return "", hash.Hash{}, errors.New("Not enough samples to build default dictionary")
	}
	defaultSamples = nil

	defaultCDict, err := gozstd.NewCDict(defaultDict)
	if err != nil {
		return "", hash.Hash{}, err
	}

	cgList, err := dagGroups.convertToChunkGroups(ctx, allChunks, defaultCDict, progress)
	sort.Slice(cgList, func(i, j int) bool {
		return cgList[i].totalBytesSavedWDict > cgList[j].totalBytesSavedWDict
	})

	// NM4 - this is helpful info to ensure that the groups are being built correctly. Maybe write to a log, or a channel.
	// for n, cg := range cgList {
	//	cg.print(n, p)
	//}

	// Allocate buffer used to compress chunks.
	cmpBuff := make([]byte, 0, maxChunkSize)

	cmpDefDict := gozstd.Compress(cmpBuff, defaultDict)
	// p("Default Dict Raw vs Compressed: %d , %d\n", len(defaultDict), len(cmpDefDict))

	arcW, err := newArchiveWriter()
	if err != nil {
		return "", hash.Hash{}, err
	}
	var defaultDictByteSpanId uint32
	defaultDictByteSpanId, err = arcW.writeByteSpan(cmpDefDict)
	if err != nil {
		return "", hash.Hash{}, err
	}

	_, grouped, singles, err := writeChunkGroupToArchive(ctx, cmpBuff, allChunks, cgList, defaultDictByteSpanId, defaultCDict, arcW)
	if err != nil {
		return "", hash.Hash{}, err
	}

	err = indexAndFinalizeArchive(arcW, archivePath)
	if err != nil {
		return "", hash.Hash{}, err
	}

	if grouped+singles != idx.chunkCount() {
		// Leaving as a panic. This should never happen.
		missing := idx.chunkCount() - (grouped + singles)
		panic(fmt.Sprintf("chunk count mismatch. Missing: %d", missing))
	}

	name, err := arcW.getName()
	if err != nil {
		return "", hash.Hash{}, err
	}

	return arcW.finalPath, name, err
}

// indexAndFinalizeArchive writes the index, metadata, and footer to the archive file. It also flushes the archive writer
// to the directory provided. The name is calculated from the footer, and can be obtained by calling getName on the archive.
func indexAndFinalizeArchive(arcW *archiveWriter, archivePath string) error {
	err := arcW.finalizeByteSpans()
	if err != nil {
		return err
	}

	err = arcW.writeIndex()
	if err != nil {
		return err
	}

	// NM4 - Pin down what we want in the metatdata.
	err = arcW.writeMetadata([]byte("{dolt_version: 0.0.0}"))
	if err != nil {
		return err
	}

	err = arcW.writeFooter()
	if err != nil {
		return err
	}

	fileName, err := arcW.genFileName(archivePath)
	if err != nil {
		return err
	}

	return arcW.flushToFile(fileName)
}

func writeChunkGroupToArchive(
	ctx context.Context,
	cmpBuff []byte,
	chunkCache *SimpleChunkSourceCache,
	cgList []*chunkGroup,
	defaultSpanId uint32,
	defaultDict *gozstd.CDict,
	arcW *archiveWriter,
) (groupCount, groupedChunkCount, individualChunkCount uint32, err error) {
	var allChunks hash.HashSet
	allChunks, err = chunkCache.addresses()
	if err != nil {
		return 0, 0, 0, err
	}

	for _, cg := range cgList {
		if cg.totalBytesSavedWDict > cg.totalBytesSavedDefaultDict {
			groupCount++

			cmpDict := gozstd.Compress(cmpBuff, cg.dict)

			dictId, err := arcW.writeByteSpan(cmpDict)
			if err != nil {
				return 0, 0, 0, err
			}

			for _, cs := range cg.chks {
				c, e2 := chunkCache.get(ctx, cs.chunkId)
				if e2 != nil {
					return 0, 0, 0, e2
				}

				if !arcW.chunkSeen(cs.chunkId) {
					compressed := gozstd.CompressDict(cmpBuff, c.Data(), cg.cDict)

					dataId, err := arcW.writeByteSpan(compressed)
					if err != nil {
						return 0, 0, 0, err
					}
					err = arcW.stageChunk(cs.chunkId, dictId, dataId)
					if err != nil {
						return 0, 0, 0, err
					}
					groupedChunkCount++
				}
				allChunks.Remove(cs.chunkId)
			}
		}
	}

	// Any chunks remaining will be written out individually.
	for h := range allChunks {
		var compressed []byte
		dictId := uint32(0)

		c, e2 := chunkCache.get(ctx, h)
		if e2 != nil {
			return 0, 0, 0, e2
		}

		compressed = gozstd.CompressDict(cmpBuff, c.Data(), defaultDict)
		dictId = defaultSpanId

		id, err := arcW.writeByteSpan(compressed)
		if err != nil {
			return 0, 0, 0, err
		}
		err = arcW.stageChunk(h, dictId, id)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	individualChunkCount = uint32(len(allChunks))

	return
}

// gatherAllChunks reads all the chunks from the chunk source and returns them in a map. The map is keyed by the hash of
// the chunk. This is going to take up a bunch of memory.
// It also returns a list of default samples, which are the first 1000 chunks that are read. These are used to build the default dictionary.
func gatherAllChunks(ctx context.Context, cs chunkSource, idx tableIndex) (*SimpleChunkSourceCache, []*chunks.Chunk, error) {
	sampleCount := min(idx.chunkCount(), maxSamples)

	defaultSamples := make([]*chunks.Chunk, 0, sampleCount)
	for i := uint32(0); i < sampleCount; i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return nil, nil, err
		}

		bytes, err := cs.get(ctx, h, nil)
		if err != nil {
			return nil, nil, err
		}

		chk := chunks.NewChunk(bytes)
		defaultSamples = append(defaultSamples, &chk)
	}

	chkCache, err := newSimpleChunkSourceCache(cs)
	if err != nil {
		return nil, nil, err
	}

	return chkCache, defaultSamples, nil
}
func verifyAllChunks(idx tableIndex, archiveFile string) error {
	file, err := os.Open(archiveFile)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	index, err := newArchiveReader(file, uint64(fileSize))
	if err != nil {
		return err
	}

	hashList := make([]hash.Hash, 0, idx.chunkCount())

	for i := uint32(0); i < idx.chunkCount(); i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return err
		}

		hashList = append(hashList, h)
	}

	rand.Shuffle(len(hashList), func(i, j int) {
		hashList[i], hashList[j] = hashList[j], hashList[i]
	})

	for _, h := range hashList {
		if !index.has(h) {
			msg := fmt.Sprintf("chunk not found in archive: %s", h.String())
			return errors.New(msg)
		}

		data, err := index.get(h)
		if err != nil {
			return fmt.Errorf("error reading chunk: %s (err: %w)", h.String(), err)
		}
		if data == nil {
			msg := fmt.Sprintf("nil data returned from archive for expected chunk: %s", h.String())
			return errors.New(msg)
		}

		chk := chunks.NewChunk(data)

		// Verify the hash of the chunk. This is the best sanity check that our data is being stored and retrieved
		// without any errors.
		if chk.Hash() != h {
			msg := fmt.Sprintf("hash mismatch for chunk: %s", h.String())
			return errors.New(msg)
		}
	}

	return nil
}

// chunkGroup is a collection of chunks that are compressed together using the same Dictionary. It also contains
// calculated statistics about the group, specifically the total compression ratio and the average raw chunk size.
type chunkGroup struct {
	dict  []byte
	cDict *gozstd.CDict
	// Sorted list of chunks and their compression score. Higher is better. The score doesn't include the dictionary size.
	chks []chunkCmpScore
	// The total ratio _includes_ the dictionary size.
	totalRatioWDict            float64
	totalBytesSavedWDict       int
	totalRatioDefaultDict      float64
	totalBytesSavedDefaultDict int
	avgRawChunkSize            int
}

// chunkCmpScore wraps a chunk and its compression score for use by the chunkGroup.
type chunkCmpScore struct {
	//chunk              *chunks.Chunk
	chunkId            hash.Hash
	score              float64
	dictCmpSize        int
	defaultDictCmpSize int
}

// newChunkGroup creates a new chunkGroup from a set of chunks.
func newChunkGroup(ctx context.Context, chunkCache *SimpleChunkSourceCache, cmpBuff []byte, chks hash.HashSet, defaultDict *gozstd.CDict) (*chunkGroup, error) {
	scored := make([]chunkCmpScore, len(chks))
	i := 0
	for h := range chks {
		scored[i] = chunkCmpScore{h, 0.0, 0, 0}
		i++
	}

	result := chunkGroup{dict: nil, cDict: nil, chks: scored}
	err := result.rebuild(ctx, chunkCache, cmpBuff, defaultDict)
	if err != nil {
		return nil, err
	}
	return &result, err
}

// For whatever reason, we need 7 or more samples to build a dictionary. But in principle we only need 1. So duplicate
// the samples until we have enough. Note that we need to add each chunk the same number of times do we don't end up
// with bias in the dictionary.
func padSamples(chks []*chunks.Chunk) []*chunks.Chunk {
	samples := []*chunks.Chunk{}
	for len(samples) < 7 {
		for _, c := range chks {
			samples = append(samples, c)
		}
	}
	return samples
}

// Add this chunk into the group. It does not attempt to determine if this is a good addition. The caller should
// use testChunk to determine if this chunk should be added.
//
// The chunkGroup will be recalculated after this chunk is added.
func (cg *chunkGroup) addChunk(ctx context.Context, chunkChache *SimpleChunkSourceCache, cmpBuff []byte, c *chunks.Chunk, defaultDict *gozstd.CDict) error {
	scored := chunkCmpScore{
		chunkId:     c.Hash(),
		score:       0.0,
		dictCmpSize: 0,
	}

	cg.chks = append(cg.chks, scored)
	return cg.rebuild(ctx, chunkChache, cmpBuff, defaultDict)
}

// worstZScore returns the z-score of the worst chunk in the group. The z-score is a measure of how many standard
// deviations from the mean the value is. This value will always be negative (If something has a positive
// z-score, it would make it better than the mean, and we don't care about that).
func (cg *chunkGroup) worstZScore() float64 {
	// Calculate the mean
	var sum float64
	for _, v := range cg.chks {
		sum += v.score
	}
	mean := sum / float64(len(cg.chks))

	// Calculate the sum of squares of differences from the mean
	var sumSquares float64
	for _, v := range cg.chks {
		diff := v.score - mean
		sumSquares += diff * diff
	}

	// Calculate the standard deviation
	stdDev := math.Sqrt(sumSquares / float64(len(cg.chks)))

	// Calculate z-score for the given value
	zScore := (cg.chks[len(cg.chks)-1].score - mean) / stdDev

	return zScore
}

// rebuild - recalculate the entire group's compression ratio. Dictionary and total compression ratio are updated as well.
// This method is called after a new chunk is added to the group. Ensures that stats about the group are up-to-date.
func (cg *chunkGroup) rebuild(ctx context.Context, chunkCache *SimpleChunkSourceCache, cmpBuff []byte, defaultDict *gozstd.CDict) error {
	chks := make([]*chunks.Chunk, len(cg.chks))

	for i, cs := range cg.chks {
		chk, err := chunkCache.get(ctx, cs.chunkId)
		if err != nil {
			return err
		}
		chks[i] = chk
	}

	dct := buildDictionary(padSamples(chks))

	var cDict *gozstd.CDict
	cDict, err := gozstd.NewCDict(dct)
	if err != nil {
		return err
	}

	cmpDct := gozstd.Compress(nil, dct)

	raw := 0
	dictCmpSize := len(cmpDct)
	noDictCmpSize := 0
	scored := make([]chunkCmpScore, len(chks))
	for i, c := range chks {
		d := c.Data()
		comp := gozstd.CompressDict(cmpBuff, d, cDict)
		defaultDictComp := gozstd.CompressDict(cmpBuff, d, defaultDict)

		ccs := chunkCmpScore{
			chunkId:            c.Hash(),
			score:              float64(len(d)-len(comp)) / float64(len(d)),
			dictCmpSize:        len(comp),
			defaultDictCmpSize: len(defaultDictComp),
		}
		scored[i] = ccs
		raw += len(c.Data())
		dictCmpSize += ccs.dictCmpSize
		noDictCmpSize += ccs.defaultDictCmpSize
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	cg.dict = dct
	cg.cDict = cDict
	cg.chks = scored

	cg.totalRatioWDict = float64(raw-dictCmpSize) / float64(raw)
	cg.totalBytesSavedWDict = raw - dictCmpSize

	cg.totalRatioDefaultDict = float64(raw-noDictCmpSize) / float64(raw)
	cg.totalBytesSavedDefaultDict = raw - noDictCmpSize

	cg.avgRawChunkSize = raw / len(chks)
	return nil
}

// Helper method to build new dictionary objects from a set of chunks.
func buildDictionary(chks []*chunks.Chunk) (ans []byte) {
	samples := make([][]uint8, 0, len(chks))
	for _, c := range chks {
		samples = append(samples, c.Data())
	}
	return gozstd.BuildDict(samples, defaultDictionarySize)
}

// Returns true if the chunk's compression ratio (using the existing dictionary) is better than the group's worst chunk.
func (cg *chunkGroup) testChunk(c *chunks.Chunk) (bool, error) {
	comp := gozstd.CompressDict(nil, c.Data(), cg.cDict)

	ratio := float64(len(c.Data())-len(comp)) / float64(len(c.Data()))

	if ratio > cg.chks[len(cg.chks)-1].score {
		return true, nil
	}
	return false, nil
}

func NewChunkRelations() ChunkRelations {
	m := make(map[hash.Hash]*hash.HashSet)
	return ChunkRelations{m}
}

// ChunkRelations is holds chunks that are related to each other. Currently the only relationship type is for chunks
// which we know are related based on modifications of a Prolly Tree, and relationships are fully transitive:
// A is related to B, and B is related to C, then A is related to C.
type ChunkRelations struct {
	// Fully transitive relationships between chunks. The key is a chunk hash, and the value is a set of chunk hashes
	// it is related to. The value is a pointer to the set, so that we can update the set in place, and many keys
	// can map to it.
	manyToGroup map[hash.Hash]*hash.HashSet
}

func (cr *ChunkRelations) Count() int {
	return len(cr.manyToGroup)
}

type ArchiveConvertToChunkGroupsMsg struct {
	GroupCount  int
	FinishedOne bool
	Done        bool
}

func (cr *ChunkRelations) convertToChunkGroups(ctx context.Context, chks *SimpleChunkSourceCache, defaultDict *gozstd.CDict, progress chan interface{}) ([]*chunkGroup, error) {
	result := make([]*chunkGroup, 0, cr.Count())

	// Create a channel for sending groups to process
	groups := cr.groups()
	progress <- ArchiveConvertToChunkGroupsMsg{GroupCount: len(groups)}

	groupChannel := make(chan hash.HashSet, len(groups))
	resultChannel := make(chan *chunkGroup, len(groups))

	// Start worker goroutines
	numThreads := 1
	var wg sync.WaitGroup
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go func() {
			buff := make([]byte, 0, maxChunkSize)
			defer wg.Done()
			for hs := range groupChannel {
				progress <- fmt.Sprintf("Worker (%d) taking group of size %d", i, len(hs))

				if len(hs) > 127 {
					progress <- "Skipping group of size > 127."
					continue
				}

				if len(hs) > 1 {
					chkGrp, err := newChunkGroup(ctx, chks, buff, hs, defaultDict)
					if err != nil {
						progress <- err
						continue
					}
					progress <- ArchiveConvertToChunkGroupsMsg{FinishedOne: true}
					resultChannel <- chkGrp
				} else {
					progress <- "Skipping single item group."
				}
			}
		}()
	}

	// Send groups to process
	for _, v := range groups {
		groupChannel <- v
	}
	close(groupChannel)

	wg.Wait()
	close(resultChannel)

	for group := range resultChannel {
		result = append(result, group)
	}

	progress <- ArchiveConvertToChunkGroupsMsg{Done: true}

	return result, nil
}

func (cr *ChunkRelations) groups() []hash.HashSet {
	seen := map[*hash.HashSet]struct{}{}
	groups := make([]hash.HashSet, 0, len(cr.manyToGroup))
	for _, v := range cr.manyToGroup {
		if _, ok := seen[v]; !ok {
			groups = append(groups, *v)
			seen[v] = struct{}{}
		}
	}
	return groups
}

func (cr *ChunkRelations) Contains(h hash.Hash) bool {
	_, ok := cr.manyToGroup[h]
	return ok
}

// Add a pair of hashes to the relations. If either chunk is already in a group, the other chunk will be added to that.
// This method has no access to the chunks themselves, so it cannot determine if the chunks are similar. This method
// is used if you know from other sources that the chunks are similar.
func (cr *ChunkRelations) Add(a, b hash.Hash) {
	_, haveA := cr.manyToGroup[a]
	_, haveB := cr.manyToGroup[b]

	if !haveA && !haveB {
		newGroup := hash.NewHashSet(a, b)
		cr.manyToGroup[a] = &newGroup
		cr.manyToGroup[b] = &newGroup
		return
	}

	if haveA && !haveB {
		cr.manyToGroup[a].Insert(b)
		cr.manyToGroup[b] = cr.manyToGroup[a]
		return
	}

	if !haveA { // haveB must be true
		cr.manyToGroup[b].Insert(a)
		cr.manyToGroup[a] = cr.manyToGroup[b]
		return
	}

	// Both are not new, and they are already in the same group.
	if cr.manyToGroup[a] == cr.manyToGroup[b] {
		return
	}

	// Both are not new, and they are in different groups. Merge the groups.
	merged := hash.NewHashSet()
	for h := range *cr.manyToGroup[a] {
		merged.Insert(h)
	}
	for h := range *cr.manyToGroup[b] {
		merged.Insert(h)
	}
	for h := range merged {
		cr.manyToGroup[h] = &merged
	}
}

type SimpleChunkSourceCache struct {
	cache *lru.TwoQueueCache[hash.Hash, *chunks.Chunk]
	cs    chunkSource
}

func newSimpleChunkSourceCache(cs chunkSource) (*SimpleChunkSourceCache, error) {
	lruCache, err := lru.New2Q[hash.Hash, *chunks.Chunk](30000000) // NM4 - Make much bigger. We'd like to stuff memory as much as possible.
	if err != nil {
		return nil, err
	}
	return &SimpleChunkSourceCache{lruCache, cs}, nil
}

// NM4 - not used currently.
func (csc *SimpleChunkSourceCache) getMany(ctx context.Context, hs hash.HashSet) ([]*chunks.Chunk, error) {
	chks := make([]*chunks.Chunk, 0, len(hs))
	records := make([]getRecord, 0, len(hs))
	for h := range hs {
		if chk, ok := csc.cache.Get(h); ok {
			chks = append(chks, chk)
		} else {
			records = append(records, getRecord{&h, h.Prefix(), false})
		}
	}
	group, ctx := errgroup.WithContext(ctx)

	allFound, err := csc.cs.getMany(ctx, group, records, func(ctx context.Context, chk *chunks.Chunk) {
		csc.cache.Add(chk.Hash(), chk)
		chks = append(chks, chk)
	}, nil)
	if err != nil {
		return nil, err
	}
	if !allFound {
		return nil, errors.New("not all chunks found")
	}

	return chks, nil
}

// NM4 - drop this and replace with getMany calls.
func (csc *SimpleChunkSourceCache) get(ctx context.Context, h hash.Hash) (*chunks.Chunk, error) {
	if chk, ok := csc.cache.Get(h); ok {
		return chk, nil
	}

	bytes, err := csc.cs.get(ctx, h, nil)
	if err != nil {
		return nil, err
	}
	chk := chunks.NewChunk(bytes)
	csc.cache.Add(h, &chk)
	return &chk, nil
}

func (csc *SimpleChunkSourceCache) has(h hash.Hash) (bool, error) {
	return csc.cs.has(h)
}

// get all the addresses as a hash.HashSet
func (csc *SimpleChunkSourceCache) addresses() (hash.HashSet, error) {
	idx, err := csc.cs.index()
	if err != nil {
		return nil, err
	}

	result := hash.NewHashSet()
	for i := uint32(0); i < idx.chunkCount(); i++ {
		var h hash.Hash
		_, err := idx.indexEntry(i, &h)
		if err != nil {
			return nil, err
		}
		result.Insert(h)
	}
	return result, nil
}
