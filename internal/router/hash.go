package router

// ShardFor hashes a shard key (e.g. user_id) to a shard ID using
// consistent hashing, so adding/removing shards reshuffles only a
// small fraction of keys instead of the whole keyspace.
//
// TODO: replace with a real hash ring (e.g. a sorted ring of virtual
// nodes per shard, binary-searched per lookup).
func ShardFor(shardKey string) string {
	panic("not implemented")
}
