package org.infinispan.query.affinity;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;

/**
 * {@link ShardDistribution} for non-clustered indexes, all shardsIdentifiers are associated with a single server.
 *
 * @since 9.0
 */
final class LocalModeShardDistribution implements ShardDistribution {

   private final Set<Integer> segments;
   private final Set<String> shardsIdentifiers;
   private final Address localAddress = LocalModeAddress.INSTANCE;

   private final Map<Integer, String> shardPerSegmentMap = new ConcurrentHashMap<>();
   private final Map<String, Set<Integer>> segmentPerShardMap = new ConcurrentHashMap<>();

   LocalModeShardDistribution(int numSegments, int numShards) {
      this.segments = IntStream.range(0, numSegments).boxed().collect(Collectors.toSet());
      this.shardsIdentifiers = IntStream.range(0, numShards).boxed().map(String::valueOf).collect(Collectors.toSet());
      this.distribute(numShards);
   }

   private void distribute(int numShards) {
      List<Set<Integer>> shardsDistribution = this.split(segments, numShards);
      int i = 0;
      for (Set<Integer> shardSegments : shardsDistribution) {
         String shardId = String.valueOf(i++);
         segmentPerShardMap.put(shardId, shardSegments);
         shardSegments.forEach(s -> shardPerSegmentMap.put(s, shardId));
      }
   }

   @Override
   public Set<String> getShardsIdentifiers() {
      return Collections.unmodifiableSet(shardsIdentifiers);
   }

   @Override
   public Address getOwner(String shardId) {
      return localAddress;
   }

   @Override
   public Set<String> getShards(Address address) {
      return Collections.unmodifiableSet(shardsIdentifiers);
   }

   @Override
   public String getShardFromSegment(Integer segment) {
      return shardPerSegmentMap.get(segment);
   }
}
