package org.infinispan.query.affinity;

import org.infinispan.distribution.ch.ConsistentHash;

/**
 * Factory for {@link ShardDistribution} instances.
 * @since 9.0
 */
final class ShardDistributionFactory {

   private ShardDistributionFactory() {
   }

   public static ShardDistribution build(int shards, int nSegments, ConsistentHash consistentHash) {
      if (consistentHash != null) {
         return shards == nSegments ? new PerSegmentShardDistribution(consistentHash) :
               new FixedShardsDistribution(consistentHash, shards);
      } else {
         return new LocalModeShardDistribution(nSegments, shards);
      }

   }

}
