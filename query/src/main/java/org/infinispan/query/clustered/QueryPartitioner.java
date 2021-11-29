package org.infinispan.query.clustered;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

/**
 * Partition segments across cluster members to satisfy a query
 *
 * @since 10.1
 */
final class QueryPartitioner {
   private final AdvancedCache<?, ?> cache;
   private final int numSegments;

   public QueryPartitioner(AdvancedCache<?, ?> cache) {
      this.cache = cache;
      ClusteringConfiguration clustering = cache.getCacheConfiguration().clustering();
      this.numSegments = clustering.hash().numSegments();
   }

   public Map<Address, BitSet> split() {
      RpcManager rpcManager = cache.getRpcManager();
      List<Address> members = rpcManager.getMembers();
      Address localAddress = rpcManager.getAddress();
      LocalizedCacheTopology cacheTopology = cache.getDistributionManager().getCacheTopology();
      BitSet bitSet = new BitSet();
      Map<Address, BitSet> segmentsPerMember = new LinkedHashMap<>(members.size());

      IntSet localSegments = cacheTopology.getLocalReadSegments();
      localSegments.stream().forEach(bitSet::set);
      segmentsPerMember.put(localAddress, bitSet);
      for (int s = 0; s < numSegments; s++) {
         if (!bitSet.get(s)) {
            Address primary = cacheTopology.getSegmentDistribution(s).primary();
            segmentsPerMember.computeIfAbsent(primary, address -> new BitSet()).set(s);
         }
      }
      return segmentsPerMember;
   }
}
