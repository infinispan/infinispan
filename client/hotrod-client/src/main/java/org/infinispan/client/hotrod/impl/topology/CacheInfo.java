package org.infinispan.client.hotrod.impl.topology;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.CacheTopologyInfoImpl;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.IntSets;

/**
 * Holds all the cluster topology information the client has for a single cache.
 *
 * <p>Each segment is mapped to a single server, even though the Hot Rod protocol and {@link ConsistentHash}
 * implementations allow more than one owner.</p>
 *
 * @author Dan Berindei
 * @since 13.0
 */
public class CacheInfo {
   private final WrappedBytes cacheName;
   private final ClusterInfo cluster;
   // The balancer is final, but using it still needs synchronization
   private final FailoverRequestBalancingStrategy balancer;
   private final int numSegments;
   private final int topologyId;
   private final List<InetSocketAddress> servers;
   private final Map<SocketAddress, Set<Integer>> primarySegments;
   private final ConsistentHash consistentHash;
   private final AtomicInteger topologyIdRef;

   public CacheInfo(WrappedBytes cacheName, FailoverRequestBalancingStrategy balancer, ClusterInfo cluster) {
      this.balancer = balancer;
      this.cluster = cluster;
      this.cacheName = cacheName;
      this.numSegments = -1;
      this.topologyId = HotRodConstants.DEFAULT_CACHE_TOPOLOGY;
      this.consistentHash = null;

      this.servers = Immutables.immutableListCopy(cluster.getInitialServers());

      // Before the first topology update or after a topology-aware (non-hash) topology update
      this.primarySegments = null;

      this.topologyIdRef = new AtomicInteger(topologyId);
   }

   public void updateBalancerServers() {
      // The servers list is immutable, so it doesn't matter that the balancer see it as a Collection<SocketAddress>
      balancer.setServers((List) servers);
   }

   public CacheInfo withNewServers(int topologyId, List<InetSocketAddress> servers) {
      return new CacheInfo(cacheName, balancer, cluster, topologyId, servers, null, -1, topologyIdRef);
   }

   public CacheInfo withNewHash(int topologyId, List<InetSocketAddress> servers, ConsistentHash consistentHash,
                                int numSegments) {
      return new CacheInfo(cacheName, balancer, cluster, topologyId, servers, consistentHash, numSegments,
                           topologyIdRef);
   }

   public CacheInfo withNewCluster(ClusterInfo newCluster, List<InetSocketAddress> servers, int tempTopologyId) {
      return new CacheInfo(cacheName, balancer, newCluster, tempTopologyId, servers, null, -1, topologyIdRef);
   }

   private CacheInfo(WrappedBytes cacheName, FailoverRequestBalancingStrategy balancer, ClusterInfo cluster,
                     int topologyId, List<InetSocketAddress> servers, ConsistentHash consistentHash,
                     int numSegments, AtomicInteger topologyIdRef) {
      this.balancer = balancer;
      this.cluster = cluster;
      this.cacheName = cacheName;
      this.numSegments = numSegments;
      this.topologyId = topologyId;
      this.consistentHash = consistentHash;
      this.topologyIdRef = topologyIdRef;

      this.servers = Immutables.immutableListCopy(servers);

      if (numSegments > 0) {
         // After the servers sent a hash-aware topology update
         this.primarySegments = consistentHash.getPrimarySegmentsByServer();
      } else {
         // Before the first topology update or after a topology-aware (non-hash) topology update
         this.primarySegments = null;
      }
   }

   public ClusterInfo getCluster() {
      return cluster;
   }

   public WrappedBytes getCacheName() {
      return cacheName;
   }

   public FailoverRequestBalancingStrategy getBalancer() {
      return balancer;
   }

   public int getNumSegments() {
      return numSegments;
   }

   public int getTopologyId() {
      return topologyId;
   }

   public AtomicInteger getTopologyIdRef() {
      return topologyIdRef;
   }

   public List<InetSocketAddress> getServers() {
      return servers;
   }

   public Map<SocketAddress, Set<Integer>> getPrimarySegments() {
      if (primarySegments == null) {
         return Collections.emptyMap();
      }

      return primarySegments;
   }

   public ConsistentHash getConsistentHash() {
      return consistentHash;
   }

   public CacheTopologyInfo getCacheTopologyInfo() {
      Map<SocketAddress, Set<Integer>> segmentsByServer;
      if (consistentHash != null) {
         segmentsByServer = consistentHash.getSegmentsByServer();
      } else {
         segmentsByServer = new HashMap<>();
         for (InetSocketAddress server : servers) {
            segmentsByServer.put(server, IntSets.immutableEmptySet());
         }
      }
      return new CacheTopologyInfoImpl(segmentsByServer,
                                       numSegments > 0 ? numSegments : null,
                                       topologyId > 0 ? topologyId : null);
   }

   public boolean isValidTopology() {
      // TODO Is it ok to consider -1 a valid topology?
      return topologyId != HotRodConstants.SWITCH_CLUSTER_TOPOLOGY;
   }
}
