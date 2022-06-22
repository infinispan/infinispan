package org.infinispan.client.hotrod.impl.topology;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.impl.CacheTopologyInfoImpl;
import org.infinispan.client.hotrod.impl.ClientTopology;
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
   private final String cacheName;
   // The topology age at the time this topology was received
   private final int topologyAge;
   // The balancer is final, but using it still needs synchronization because it is not thread-safe
   private final FailoverRequestBalancingStrategy balancer;
   private final int numSegments;
   private final List<InetSocketAddress> servers;
   private final Map<SocketAddress, Set<Integer>> primarySegments;
   private final ConsistentHash consistentHash;
   private final AtomicReference<ClientTopology> clientTopologyRef;
   private final ClientTopology clientTopology;

   public CacheInfo(WrappedBytes cacheName, FailoverRequestBalancingStrategy balancer, int topologyAge, List<InetSocketAddress> servers, ClientIntelligence intelligence) {
      this.balancer = balancer;
      this.topologyAge = topologyAge;
      this.cacheName = cacheName == null || cacheName.getLength() == 0 ? "<default>" :
                       new String(cacheName.getBytes(), HotRodConstants.HOTROD_STRING_CHARSET);
      this.numSegments = -1;
      this.clientTopology = new ClientTopology(HotRodConstants.DEFAULT_CACHE_TOPOLOGY, intelligence);
      this.consistentHash = null;

      this.servers = Immutables.immutableListCopy(servers);

      // Before the first topology update or after a topology-aware (non-hash) topology update
      this.primarySegments = null;

      clientTopologyRef = new AtomicReference<>(clientTopology);
   }

   public void updateBalancerServers() {
      // The servers list is immutable, so it doesn't matter that the balancer see it as a Collection<SocketAddress>
      balancer.setServers((List) servers);
   }

   public CacheInfo withNewServers(int topologyAge, int topologyId, List<InetSocketAddress> servers) {
      return withNewServers(topologyAge, topologyId, servers, clientTopologyRef.get().getClientIntelligence());
   }

   public CacheInfo withNewServers(int topologyAge, int topologyId, List<InetSocketAddress> servers, ClientIntelligence intelligence) {
      return new CacheInfo(cacheName, balancer, topologyAge,  servers, null, -1, clientTopologyRef, new ClientTopology(topologyId, intelligence));
   }

   public CacheInfo withNewHash(int topologyAge, int topologyId, List<InetSocketAddress> servers,
                                ConsistentHash consistentHash, int numSegments) {
      return new CacheInfo(cacheName, balancer, topologyAge, servers, consistentHash, numSegments, clientTopologyRef, new ClientTopology(topologyId, getIntelligence()));
   }

   private CacheInfo(String cacheName, FailoverRequestBalancingStrategy balancer, int topologyAge,
                     List<InetSocketAddress> servers, ConsistentHash consistentHash, int numSegments,
                     AtomicReference<ClientTopology> clientTopologyRef, ClientTopology clientTopology) {
      this.balancer = balancer;
      this.topologyAge = topologyAge;
      this.cacheName = cacheName;
      this.numSegments = numSegments;
      this.consistentHash = consistentHash;
      this.clientTopology = clientTopology;
      this.clientTopologyRef = clientTopologyRef;

      this.servers = Immutables.immutableListCopy(servers);

      if (numSegments > 0) {
         // After the servers sent a hash-aware topology update
         this.primarySegments = consistentHash.getPrimarySegmentsByServer();
      } else {
         // Before the first topology update or after a topology-aware (non-hash) topology update
         this.primarySegments = null;
      }
   }

   public String getCacheName() {
      return cacheName;
   }

   public int getTopologyAge() {
      return topologyAge;
   }

   public FailoverRequestBalancingStrategy getBalancer() {
      return balancer;
   }

   public int getNumSegments() {
      return numSegments;
   }

   public int getTopologyId() {
      return clientTopology.getTopologyId();
   }

   public AtomicReference<ClientTopology> getClientTopologyRef() {
      return clientTopologyRef;
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
         segmentsByServer = new HashMap<>(servers.size());
         for (InetSocketAddress server : servers) {
            segmentsByServer.put(server, IntSets.immutableEmptySet());
         }
      }
      return new CacheTopologyInfoImpl(segmentsByServer,
                                       numSegments > 0 ? numSegments : null,
                                       getTopologyId());
   }

   private ClientIntelligence getIntelligence() {
      return clientTopology.getClientIntelligence();
   }

   public void updateClientTopologyRef() {
      clientTopologyRef.set(clientTopology);
   }
}
