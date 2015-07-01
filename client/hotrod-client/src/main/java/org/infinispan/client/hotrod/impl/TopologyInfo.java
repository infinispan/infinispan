package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Immutables;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.infinispan.commons.util.InfinispanCollections.emptySet;

/**
 * Maintains topology information about caches.
 *
 * @author gustavonalle
 */
public final class TopologyInfo {

   private static final Log log = LogFactory.getLog(TopologyInfo.class, Log.class);

   private Collection<SocketAddress> servers = new ArrayList<>();
   private Map<byte[], ConsistentHash> consistentHashes = CollectionFactory.makeMap(ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance());
   private Map<byte[], Integer> segmentsByCache = CollectionFactory.makeMap(ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance());
   private volatile AtomicInteger topologyId;
   private final ConsistentHashFactory hashFactory = new ConsistentHashFactory();

   public TopologyInfo(AtomicInteger topologyId, Collection<SocketAddress> servers, Configuration configuration) {
      this.topologyId = topologyId;
      this.servers = servers;
      hashFactory.init(configuration);
   }

   private Map<SocketAddress, Set<Integer>> getSegmentsByServer(byte[] cacheName) {
      ConsistentHash consistentHash = consistentHashes.get(cacheName);
      if (consistentHash != null) {
         return consistentHash.getSegmentsByServer();
      } else {
         Optional<Integer> numSegments = Optional.ofNullable(segmentsByCache.get(cacheName));
         Optional<Set<Integer>> segments = numSegments.map(n -> range(0, n).boxed().collect(Collectors.toSet()));
         return Immutables.immutableMapWrap(
               servers.stream().collect(toMap(identity(), s -> segments.orElse(emptySet())))
         );
      }
   }

   public Collection<SocketAddress> getServers() {
      return servers;
   }

   public void updateTopology(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, short hashFunctionVersion, int hashSpace, byte[] cacheName) {
      ConsistentHash hash = hashFactory.newConsistentHash(hashFunctionVersion);
      if (hash == null) {
         log.noHasHFunctionConfigured(hashFunctionVersion);
      } else {
         hash.init(servers2Hash, numKeyOwners, hashSpace);
      }
      consistentHashes.put(cacheName, hash);

   }

   public void updateTopology(SocketAddress[][] segmentOwners, int numSegments, short hashFunctionVersion, byte[] cacheName) {
      if (hashFunctionVersion > 0) {
         SegmentConsistentHash hash = hashFactory.newConsistentHash(hashFunctionVersion);
         if (hash == null) {
            log.noHasHFunctionConfigured(hashFunctionVersion);
         } else {
            hash.init(segmentOwners, numSegments);
         }
         consistentHashes.put(cacheName, hash);
      }
      segmentsByCache.put(cacheName, numSegments);

   }

   public Optional<SocketAddress> getHashAwareServer(byte[] key, byte[] cacheName) {
      Optional<SocketAddress> server = Optional.empty();
      ConsistentHash consistentHash = consistentHashes.get(cacheName);
      if (consistentHash != null) {
         server = Optional.of(consistentHash.getServer(key));
         if (log.isTraceEnabled()) {
            log.tracef("Using consistent hash for determining the server: " + server);
         }
      }
      return server;
   }

   public void updateServers(Collection<SocketAddress> updatedServers) {
      servers = updatedServers;
   }

   public ConsistentHash getConsistentHash(byte[] cacheName) {
      return consistentHashes.get(cacheName);
   }

   public ConsistentHashFactory getConsistentHashFactory() {
      return hashFactory;
   }

   public void setTopologyId(int topologyId) {
      this.topologyId.set(topologyId);
   }

   public CacheTopologyInfo getCacheTopologyInfo(byte[] cacheName) {
      return new CacheTopologyInfoImpl(getSegmentsByServer(cacheName), segmentsByCache.get(cacheName), topologyId.get());
   }

}
