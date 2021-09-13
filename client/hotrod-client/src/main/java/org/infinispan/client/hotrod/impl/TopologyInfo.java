package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.impl.Util.wrapBytes;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.topology.ClusterInfo;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;

import net.jcip.annotations.NotThreadSafe;

/**
 * Maintains topology information about caches.
 *
 * @author gustavonalle
 * @author Dan Berindei
 */
@NotThreadSafe
public final class TopologyInfo {

   private static final Log log = LogFactory.getLog(TopologyInfo.class, Log.class);

   private final Supplier<FailoverRequestBalancingStrategy> balancerFactory;
   private final ConsistentHashFactory hashFactory = new ConsistentHashFactory();

   private final ConcurrentMap<WrappedBytes, CacheInfo> caches = new ConcurrentHashMap<>();
   private volatile ClusterInfo cluster;
   // Topology age provides a way to avoid concurrent cluster view changes,
   // affecting a cluster switch. After a cluster switch, the topology age is
   // increased and so any old requests that might have received topology
   // updates won't be allowed to apply since they refer to older views.
   private int topologyAge;

   public TopologyInfo(Configuration configuration, ClusterInfo clusterInfo) {
      this.balancerFactory = configuration.balancingStrategyFactory();
      this.hashFactory.init(configuration);

      this.topologyAge = 0;
      this.cluster = clusterInfo;
   }

   public Map<SocketAddress, Set<Integer>> getPrimarySegmentsByServer(byte[] cacheName) {
      WrappedByteArray key = wrapBytes(cacheName);
      CacheInfo cacheInfo = caches.get(key);
      if (cacheInfo != null) {
         return cacheInfo.getPrimarySegments();
      } else {
         return Collections.emptyMap();
      }
   }

   public List<InetSocketAddress> getServers(WrappedBytes cacheName) {
      return getCacheInfo(cacheName).getServers();
   }

   public Collection<InetSocketAddress> getAllServers() {
      return caches.values().stream().flatMap(ct -> ct.getServers().stream()).collect(Collectors.toSet());
   }

   public SegmentConsistentHash createConsistentHash(int numSegments, short hashFunctionVersion,
                                                     SocketAddress[][] segmentOwners) {
      SegmentConsistentHash hash = null;
      if (hashFunctionVersion > 0) {
         hash = hashFactory.newConsistentHash(hashFunctionVersion);
         if (hash == null) {
            HOTROD.noHasHFunctionConfigured(hashFunctionVersion);
         } else {
            hash.init(segmentOwners, numSegments);
         }
      }
      return hash;
   }

   public ConsistentHashFactory getConsistentHashFactory() {
      return hashFactory;
   }

   public CacheTopologyInfo getCacheTopologyInfo(byte[] cacheName) {
      WrappedByteArray key = wrapBytes(cacheName);
      return caches.get(key).getCacheTopologyInfo();
   }

   public CacheInfo getCacheInfo(WrappedBytes cacheName) {
      return caches.get(cacheName);
   }

   public CacheInfo getOrCreateCacheInfo(WrappedBytes cacheName) {
      return caches.computeIfAbsent(cacheName, cn -> {
         CacheInfo cacheInfo = new CacheInfo(cn, balancerFactory.get(), topologyAge, cluster.getInitialServers());
         cacheInfo.updateBalancerServers();
         if (log.isTraceEnabled()) log.tracef("Creating cache info %s with topology age %d",
                                              cacheInfo.getCacheName(), topologyAge);
         return cacheInfo;
      });
   }

   /**
    * Switch the cluster or reset to the initial server list of the current cluster.
    *
    * <p>Does not itself reset the topology id and server list of individual caches.
    * New operations will send the incremented topology age to the server,
    * and </p>
    */
   public void switchCluster(ClusterInfo newCluster) {
      if (log.isTraceEnabled())
         log.tracef("Switching cluster: %s -> %s", cluster.getName(), newCluster.getName());

      // Stop accepting topology updates from old requests
      caches.replaceAll((name, oldInfo) -> {
         CacheInfo newInfo = oldInfo.withNewServers(topologyAge + 1, HotRodConstants.SWITCH_CLUSTER_TOPOLOGY,
                                                    newCluster.getInitialServers());
         // Updates the balancer in both infos
         newInfo.updateBalancerServers();
         // Update the topology ref for both infos and ongoing operations
         newInfo.getTopologyIdRef().set(HotRodConstants.SWITCH_CLUSTER_TOPOLOGY);
         return newInfo;
      });

      // Actual cluster switch
      cluster = newCluster;
      // Increment the topology age for new requests so that the topology updates from their responses are accepted
      topologyAge++;
   }

   /**
    * Reset a single ache to the initial server list.
    *
    * <p>Useful if there are still live servers in the cluster, but all the server in this cache's
    * current topology are unreachable.</p>
    */
   public void reset(WrappedBytes cacheName) {
      if (log.isTraceEnabled()) log.tracef("Switching to initial server list for cache %s, cluster %s",
                                           cacheName, cluster.getName());
      caches.computeIfPresent(cacheName, (name, oldInfo) -> {
         CacheInfo newInfo = oldInfo.withNewServers(topologyAge, HotRodConstants.DEFAULT_CACHE_TOPOLOGY,
                                                    cluster.getInitialServers());
         // Updates the balancer in both infos
         newInfo.updateBalancerServers();
         // Update the topology ref for both infos and ongoing operations
         newInfo.getTopologyIdRef().set(newInfo.getTopologyId());
         return newInfo;
      });
   }

   public ClusterInfo getCluster() {
      return cluster;
   }

   public int getTopologyAge() {
      return topologyAge;
   }

   public void updateCacheInfo(WrappedBytes cacheName, CacheInfo oldCacheInfo, CacheInfo newCacheInfo) {
      if (log.isTraceEnabled()) log.tracef("Updating topology for %s: %s -> %s", newCacheInfo.getCacheName(),
                                           oldCacheInfo.getTopologyId(), newCacheInfo.getTopologyId());
      CacheInfo existing = caches.put(cacheName, newCacheInfo);
      assert existing == oldCacheInfo : "Locking should have prevented concurrent updates";

      // The new CacheInfo doesn't have a new balancer instance, so the server update affects both
      newCacheInfo.updateBalancerServers();
   }

   public void forEachCache(BiConsumer<WrappedBytes, CacheInfo> action) {
      caches.forEach(action);
   }
}
