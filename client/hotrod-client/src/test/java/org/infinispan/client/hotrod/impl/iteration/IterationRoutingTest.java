package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.ServerAddress;
import org.testng.annotations.Test;

/**
 * Test for picking the server to start the iteration where the majority of segments are located.
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.IterationRoutingTest")
public class IterationRoutingTest extends MultiHotRodServersTest {

   private static final int NUM_SERVERS = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numSegments(20).numOwners(2);
      return builder;
   }

   @Test
   public void testIterationRouting() {
      for (RemoteCacheManager cacheManager : clients) {
         RemoteCache<Integer, AccountHS> remoteCache = cacheManager.getCache();
         CacheTopologyInfo cacheTopologyInfo = remoteCache.getCacheTopologyInfo();
         Map<SocketAddress, Set<Integer>> segmentsPerServer = cacheTopologyInfo.getSegmentsPerServer();
         segmentsPerServer.forEach((serverAddress, ownedSegments) -> {
            // Trying to retrieve all segments owned by a server should route the iteration to that server
            try (CloseableIterator<Map.Entry<Object, Object>> ignored = remoteCache.retrieveEntries(null, ownedSegments, 10)) {
               assertIterationActiveOnServer((InetSocketAddress) serverAddress);
            }

            assertNoActiveIterations();

            // Trying to retrieve segments owned by 3 servers should route to the server that has more segments
            Set<Integer> mixedSegments = getMajoritySegmentsOwnedBy(asSocketAddress(serverAddress), cacheTopologyInfo);

            try (CloseableIterator<Map.Entry<Object, Object>> ignored = remoteCache.retrieveEntries(null, mixedSegments, 10)) {
               assertIterationActiveOnServer((InetSocketAddress) serverAddress);
            }
         });
      }
   }

   private Set<Integer> getMajoritySegmentsOwnedBy(ServerAddress majorityServer, CacheTopologyInfo cacheTopologyInfo) {
      Map<SocketAddress, Set<Integer>> segmentsPerServer = cacheTopologyInfo.getSegmentsPerServer();
      Set<Integer> results = new HashSet<>();

      for (HotRodServer server : servers) {
         ServerAddress serverAddress = server.getAddress();
         Set<Integer> segs = segmentsPerServer.get(asServerAddress(serverAddress));
         if (serverAddress.equals(majorityServer)) {
            results.addAll(segs);
         } else {
            results.addAll(segs.stream().limit(2).collect(Collectors.toSet()));
         }
      }
      return results;
   }

   private InetSocketAddress asServerAddress(ServerAddress address) {
      return InetSocketAddress.createUnresolved(address.getHost(), address.getPort());
   }

   private ServerAddress asSocketAddress(SocketAddress socketAddress) {
      InetSocketAddress isa = (InetSocketAddress) socketAddress;
      return new ServerAddress(isa.getHostName(), isa.getPort());
   }

   private void assertNoActiveIterations() {
      servers.forEach(h -> assertEquals(0, h.getIterationManager().activeIterations()));
   }

   private void assertIterationActiveOnServer(InetSocketAddress address) {
      for (HotRodServer server : servers) {
         String host = server.getAddress().getHost();
         int port = server.getAddress().getPort();
         int activeIterations = server.getIterationManager().activeIterations();
         if (host.equals(address.getHostName()) && port == address.getPort()) {
            assertEquals(1L, activeIterations);
         } else {
            assertEquals(0L, activeIterations);
         }
      }
   }
}
