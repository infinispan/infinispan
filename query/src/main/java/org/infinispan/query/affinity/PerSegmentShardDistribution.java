package org.infinispan.query.affinity;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.LogFactory;

/**
 * Allocates one index shard per Infinispan segment, with the shard identifier equals to the segment.
 *
 * @since 9.0
 */
class PerSegmentShardDistribution implements ShardDistribution {

   private static final Log logger = LogFactory.getLog(PerSegmentShardDistribution.class, Log.class);

   private final Set<String> identifiers;
   private final ConsistentHash consistentHash;

   PerSegmentShardDistribution(ConsistentHash consistentHash) {
      int numSegments = consistentHash.getNumSegments();
      this.consistentHash = consistentHash;
      this.identifiers = IntStream.range(0, numSegments).boxed().map(String::valueOf).collect(Collectors.toSet());
      logger.debugf("Created with numSegments %d", numSegments);
   }

   @Override
   public Set<String> getShardsIdentifiers() {
      return Collections.unmodifiableSet(identifiers);
   }

   @Override
   public Address getOwner(String shardId) {
      return consistentHash.locatePrimaryOwnerForSegment(Integer.valueOf(shardId));
   }

   @Override
   public Set<String> getShards(Address address) {
      return consistentHash.getPrimarySegmentsForOwner(address).stream()
            .map(String::valueOf).collect(Collectors.toSet());
   }

   @Override
   public String getShardFromSegment(Integer segment) {
      return String.valueOf(segment);
   }
}
