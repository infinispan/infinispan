package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.impl.Util.wrapBytes;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;
import org.infinispan.client.hotrod.impl.topology.ClusterInfo;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;

/**
 * Maintains topology information about caches.
 *
 * @author gustavonalle
 */
public final class TopologyInfo {

   private static final Log log = LogFactory.getLog(TopologyInfo.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Supplier<FailoverRequestBalancingStrategy> balancerFactory;
   private final ConsistentHashFactory hashFactory = new ConsistentHashFactory();

   private final ConcurrentMap<WrappedBytes, CacheInfo> caches = new ConcurrentHashMap<>();
   private volatile ClusterInfo currentCluster;
   // Topology age provides a way to avoid concurrent cluster view changes,
   // affecting a cluster switch. After a cluster switch, the topology age is
   // increased and so any old requests that might have received topology
   // updates won't be allowed to apply since they refer to older views.
   private int topologyAge;

   public TopologyInfo(Configuration configuration, ClusterInfo clusterInfo) {
      this.balancerFactory = configuration.balancingStrategyFactory();
      this.hashFactory.init(configuration);

      this.topologyAge = 0;
      this.currentCluster = clusterInfo;
   }

   public List<InetSocketAddress> getServers(WrappedBytes cacheName) {
      return getCacheInfo(cacheName).getServers();
   }

   public Collection<InetSocketAddress> getCurrentServers() {
      return caches.values().stream().flatMap(ct -> ct.getServers().stream()).collect(Collectors.toSet());
   }

   public SegmentConsistentHash createConsistentHash(int numSegments, short hashFunctionVersion,
                                                     SocketAddress[][] segmentOwners) {
      SegmentConsistentHash hash = null;
      if (hashFunctionVersion > 0) {
         hash = hashFactory.newConsistentHash(hashFunctionVersion);
         if (hash == null) {
            log.noHasHFunctionConfigured(hashFunctionVersion);
         } else {
            hash.init(segmentOwners, numSegments);
         }
      }
      return hash;
   }

   public ConsistentHash createConsistentHash1x(Map<InetSocketAddress, Set<Integer>> servers2Hash, int numKeyOwners,
                                                short hashFunctionVersion, int hashSpace) {
      ConsistentHash hash = null;
      if (hashFunctionVersion > 0) {
         hash = hashFactory.newConsistentHash(hashFunctionVersion);
         if (hash == null) {
            log.noHasHFunctionConfigured(hashFunctionVersion);
         } else {
            hash.init((Map)servers2Hash, numKeyOwners, hashSpace);
         }
      }
      return hash;
   }

   public SocketAddress getHashAwareServer(Object key, byte[] cacheName) {
      CacheInfo cacheInfo = caches.get(wrapBytes(cacheName));
      if (cacheInfo != null && cacheInfo.isValidTopology() && cacheInfo.getConsistentHash() != null) {
         return cacheInfo.getConsistentHash().getServer(key);
      }

      return null;
   }

   private boolean isReplicated(Map<SocketAddress, Set<Integer>> ch) {
      if(ch.isEmpty()) return false;
      Set<Integer> first = ch.values().iterator().next();
      return ch.values().stream().allMatch(s -> s.equals(first));
   }

   /**
    * Returns an address that is an owner, either primary or backup, of the most amount of segments from the provided
    * set of segments.
    * @param segments desired segments
    * @param cacheName cache to check
    * @return an address that should be contacted for the given segments or empty if the topology is not valid
    */
   public SocketAddress getHashAwareServer(Set<Integer> segments, byte[] cacheName) {
      SocketAddress server = null;
      if(segments == null || segments.isEmpty()) return server;
      if (isTopologyValid(cacheName)) {
         ConsistentHash consistentHash = caches.get(new WrappedByteArray(cacheName)).getConsistentHash();
         if (consistentHash != null) {
            Map<SocketAddress, Set<Integer>> segmentsByServer = consistentHash.getSegmentsByServer();
            if (isReplicated(segmentsByServer)) {
               // For replicated caches, it does not matter which server is picked, since all
               // servers have all segments, but it's better to avoid always picking the first
               Integer firstSegment = segments.iterator().next();
               int index = firstSegment % segmentsByServer.size();
               SocketAddress replicatedServer = segmentsByServer.keySet().stream().skip(index).findFirst().orElse(null);
               if (trace) {
                  log.tracef("Remote cache is replicated, choosing server %s for segments %s", replicatedServer, segments);
               }
               return replicatedServer;
            }
            final int segmentMax = segments.size();
            int currentMax = 0;
            SocketAddress socketAddressToUse = null;

            for (Map.Entry<SocketAddress, Set<Integer>> serverSegment : segmentsByServer.entrySet()) {
               int segmentOwnedCount = 0;
               for (Integer segment : serverSegment.getValue()) {
                  if (segments.contains(segment)) {
                     segmentOwnedCount++;
                  }
               }
               // Means the current server owns all segments so just use it
               if (segmentOwnedCount == segmentMax) {
                  socketAddressToUse = serverSegment.getKey();
                  break;
               }
               // Prefer the server that owns the most segments
               if (segmentOwnedCount > currentMax) {
                  currentMax = segmentOwnedCount;
                  socketAddressToUse = serverSegment.getKey();
               }
            }

            if (socketAddressToUse != null) {
               server = socketAddressToUse;
            }

            if (trace) {
               log.tracef("Using consistent hash for determining the server: " + socketAddressToUse +
                     " as it owns " + currentMax + " of the " + segmentMax + " provided segments.");
            }
         }
      }

      return server;
   }

   public boolean isTopologyValid(byte[] cacheName) {
      CacheInfo cacheInfo = caches.get(wrapBytes(cacheName));

      return cacheInfo != null && cacheInfo.isValidTopology();
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

   public CacheInfo createCacheInfo(WrappedBytes cacheName) {
      CacheInfo cacheInfo = new CacheInfo(cacheName, balancerFactory.get(), currentCluster);
      cacheInfo.updateBalancerServers();
      caches.put(cacheName, cacheInfo);
      return cacheInfo;
   }

   public CacheInfo getOrCreateCacheInfo(WrappedBytes cacheName) {
      return caches.computeIfAbsent(cacheName, cn -> {
         CacheInfo cacheInfo = new CacheInfo(cn, balancerFactory.get(), currentCluster);
         // Copy the servers and consistent hash from the default cache, if it exists
         CacheInfo defaultCacheInfo = caches.get(WrappedByteArray.EMPTY_BYTES);
         if (defaultCacheInfo != null) {
            cacheInfo = cacheInfo.withNewHash(-1, defaultCacheInfo.getServers(),
                                              defaultCacheInfo.getConsistentHash(), defaultCacheInfo.getNumSegments());
         }
         cacheInfo.updateBalancerServers();
         return cacheInfo;
      });
   }

   /**
    * Switch the cluster or reset to the initial server list of the current cluster.
    *
    * <p>Reset the topology id and server list of all caches.</p>
    */
   public void switchCluster(ClusterInfo newCluster, WrappedBytes cacheName) {
      if (log.isTraceEnabled()) log.tracef("Updating cluster for %s: %s -> %s",
                                           cacheName, currentCluster.getName(), newCluster.getName());
      // FIXME We need this hack because the switch to the initial server list can be triggered before the cluster switch
      int tempTopologyId = (newCluster != currentCluster) ? HotRodConstants.SWITCH_CLUSTER_TOPOLOGY : -1;
      // TODO Remove the cacheName parameter?
      topologyAge++;
      currentCluster = newCluster;

      // TODO Maybe it 's enough to update the topology age here and to update the cache infos lazily
      //  as we get new operations and they notice the topology age is outdated?
      //  We'd have to keep track of topologyAge in CacheInfo for that to work
      //  The old code (pre-ISPN-13264) only updated the topology id and server list for the cache
      //  given as a parameter, but that seems wrong
      caches.replaceAll((name, oldInfo) -> {
         CacheInfo newInfo = oldInfo.withNewCluster(currentCluster, currentCluster.getInitialServers(), tempTopologyId);
         // Updates the balancer in both infos
         newInfo.updateBalancerServers();
         // Update the topology ref for both infos and ongoing operations
         newInfo.getTopologyIdRef().set(newInfo.getTopologyId());
         return newInfo;
      });
   }

   public ClusterInfo getCurrentCluster() {
      return currentCluster;
   }

   public int getTopologyAge() {
      return topologyAge;
   }

   public void updateCacheInfo(WrappedBytes cacheName, CacheInfo oldCacheInfo, CacheInfo newCacheInfo) {
      if (log.isTraceEnabled()) log.tracef("Updating topology for %s: %s -> %s",
                                           cacheName, oldCacheInfo.getTopologyId(), newCacheInfo.getTopologyId());
      CacheInfo existing = caches.put(cacheName, newCacheInfo);
      assert existing == oldCacheInfo : "Locking should have prevented concurrent updates";

      // The new CacheInfo doesn't have a new balancer instance, so the server update affects both
      newCacheInfo.updateBalancerServers();
   }
}
