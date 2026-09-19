package router

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring is a consistent-hash ring mapping shard keys to shard IDs.
//
// Each shard occupies multiple virtual nodes spread around the ring
// so that adding or removing one shard only reshuffles the keys that
// land near its vnodes, not the whole keyspace.
type Ring struct {
	mu           sync.RWMutex
	vnodes       int
	sortedHashes []uint32
	hashToShard  map[uint32]string
}

func NewRing(vnodesPerShard int) *Ring {
	if vnodesPerShard <= 0 {
		vnodesPerShard = 100
	}
	return &Ring{
		vnodes:      vnodesPerShard,
		hashToShard: make(map[uint32]string),
	}
}

// AddShard places shardID's virtual nodes on the ring. Safe to call
// again for the same shardID — any vnode hash already present (from
// this shard or, extremely rarely, a collision with another) is left
// as-is rather than clobbered.
func (r *Ring) AddShard(shardID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.vnodes; i++ {
		h := hashKey(shardID + "#" + strconv.Itoa(i))
		if _, exists := r.hashToShard[h]; exists {
			continue
		}
		r.hashToShard[h] = shardID
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
}

// RemoveShard takes shardID's virtual nodes off the ring.
func (r *Ring) RemoveShard(shardID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.sortedHashes[:0]
	for _, h := range r.sortedHashes {
		if r.hashToShard[h] == shardID {
			delete(r.hashToShard, h)
			continue
		}
		kept = append(kept, h)
	}
	r.sortedHashes = kept
}

// ShardFor returns the shard owning shardKey: the first vnode at or
// after hash(shardKey) walking clockwise around the ring, wrapping to
// the first vnode if the key hashes past the last one. ok is false
// only when the ring has no shards registered.
func (r *Ring) ShardFor(shardKey string) (shardID string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return "", false
	}
	h := hashKey(shardKey)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })
	if idx == len(r.sortedHashes) {
		idx = 0
	}
	return r.hashToShard[r.sortedHashes[idx]], true
}

func hashKey(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}
