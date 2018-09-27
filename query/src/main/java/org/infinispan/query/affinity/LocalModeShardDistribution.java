package org.infinispan.query.affinity;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;

/**
 * {@link ShardDistribution} for non-clustered indexes, all shard identifiers are associated with a single server.
 *
 * @since 9.0
 */
final class LocalModeShardDistribution implements ShardDistribution {

   private final int numShards;

   private final Set<String> shardIds;

   LocalModeShardDistribution(int numShards) {
      this.numShards = numShards;
      this.shardIds = Collections.unmodifiableSet(IntStream.range(0, numShards).boxed().map(String::valueOf).collect(Collectors.toSet()));
   }

   @Override
   public Set<String> getShardsIdentifiers() {
      return shardIds;
   }

   @Override
   public Address getOwner(String shardId) {
      return LocalModeAddress.INSTANCE;
   }

   @Override
   public Set<String> getShards(Address address) {
      return shardIds;
   }

   @Override
   public String getShardFromSegment(int segment) {
      return String.valueOf(segment % numShards);
   }
}
