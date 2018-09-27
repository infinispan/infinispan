package org.infinispan.query.affinity;

import java.util.Set;

import org.infinispan.remoting.transport.Address;

/**
 * Provides shard distribution information for an index.
 *
 * @since 9.0
 */
interface ShardDistribution {

   /**
    * @return All shards identifiers.
    */
   Set<String> getShardsIdentifiers();

   /**
    * @return Owner for a single shard.
    */
   Address getOwner(String shardId);

   /**
    * @return All shards owned by a node.
    */
   Set<String> getShards(Address address);

   /**
    * @return the shard mapped to a certain segment of the Infinispan consistent hash
    */
   String getShardFromSegment(int segment);
}
