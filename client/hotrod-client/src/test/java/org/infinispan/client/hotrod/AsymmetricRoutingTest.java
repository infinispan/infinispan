package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.blockUntilCacheStatusAchieved;
import static org.infinispan.test.TestingUtil.blockUntilViewReceived;
import static org.testng.AssertJUnit.assertEquals;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.AsymmetricRoutingTest")
public class AsymmetricRoutingTest extends HitsAwareCacheManagersTest {

   private static final String DIST_ONE_CACHE_NAME = "dist-one-cache";
   private static final String DIST_TWO_CACHE_NAME = "dist-two-cache";

   HotRodServer server1;
   HotRodServer server2;

   ConfigurationBuilder defaultBuilder;
   ConfigurationBuilder distOneBuilder;
   ConfigurationBuilder distTwoBuilder;

   RemoteCacheManager rcm;

   protected ConfigurationBuilder defaultCacheConfigurationBuilder() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      defaultBuilder = defaultCacheConfigurationBuilder();
      distOneBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      distOneBuilder.clustering().hash().numOwners(1).numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory.Default(0));
      distTwoBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      distTwoBuilder.clustering().hash().numOwners(1).numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory.Default(1));

      server1 = addHotRodServer();
      server2 = addHotRodServer();

      blockUntilViewReceived(manager(0).getCache(), 2);
      blockUntilCacheStatusAchieved(manager(0).getCache(), ComponentStatus.RUNNING, 10000);
      blockUntilCacheStatusAchieved(manager(1).getCache(), ComponentStatus.RUNNING, 10000);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host(server1.getHost()).port(server1.getPort())
            .addServer().host(server2.getHost()).port(server2.getPort());
      rcm = new RemoteCacheManager(clientBuilder.build());
   }

   @AfterClass
   @Override
   protected void destroy() {
      killRemoteCacheManager(rcm);
      killServers(server1, server2);
      server1 = null;
      server2 = null;
      super.destroy();
   }

   private HotRodServer addHotRodServer() {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(ControlledConsistentHashFactory.SCI.INSTANCE, defaultBuilder);
      cm.defineConfiguration(DIST_ONE_CACHE_NAME, distOneBuilder.build());
      cm.defineConfiguration(DIST_TWO_CACHE_NAME, distTwoBuilder.build());
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm);
      addr2hrServer.put(new InetSocketAddress(server.getHost(), server.getPort()), server);
      return server;
   }

   public void testRequestRouting() {
      addInterceptors(DIST_ONE_CACHE_NAME);
      addInterceptors(DIST_TWO_CACHE_NAME);
      byte[] keyDistOne = getKeyForServer(server1, DIST_ONE_CACHE_NAME);
      byte[] keyDistTwo = getKeyForServer(server2, DIST_TWO_CACHE_NAME);
      assertSegments(DIST_ONE_CACHE_NAME, server1, server1.getCacheManager().getAddress());
      assertSegments(DIST_ONE_CACHE_NAME, server2, server1.getCacheManager().getAddress());
      assertSegments(DIST_TWO_CACHE_NAME, server1, server2.getCacheManager().getAddress());
      assertSegments(DIST_TWO_CACHE_NAME, server2, server2.getCacheManager().getAddress());
      assertRequestRouting(keyDistOne, DIST_ONE_CACHE_NAME, server1);
      assertRequestRouting(keyDistTwo, DIST_TWO_CACHE_NAME, server2);
   }

   private void assertSegments(String cacheName, HotRodServer server, Address owner) {
      AdvancedCache<Object, Object> cache = server.getCacheManager().getCache(cacheName).getAdvancedCache();
      ConsistentHash ch = cache.getDistributionManager().getReadConsistentHash();
      assertEquals(1, ch.getNumSegments());
      Set<Integer> segments = ch.getSegmentsForOwner(owner);
      assertEquals(1, segments.size());
      assertEquals(0, segments.iterator().next().intValue());
   }

   private void assertRequestRouting(byte[] key, String cacheName, HotRodServer server) {
      RemoteCache<Object, Object> rcOne = rcm.getCache(cacheName);
      InetSocketAddress serverAddress = new InetSocketAddress(server.getHost(), server.getPort());
      for (int i = 0; i < 2; i++) {
         log.infof("Routing put test for key %s", Util.printArray(key, false));
         rcOne.put(key, "value");
         assertServerHit(serverAddress, cacheName, i + 1);
      }
   }

   byte[] getKeyForServer(HotRodServer primaryOwner, String cacheName) {
      Cache<?, ?> cache = primaryOwner.getCacheManager().getCache(cacheName);
      Random r = new Random();
      byte[] dummy = new byte[8];
      int attemptsLeft = 1000;
      do {
         r.nextBytes(dummy);
         attemptsLeft--;
      } while (!isFirstOwner(cache, dummy) && attemptsLeft >= 0);

      if (attemptsLeft < 0)
         throw new IllegalStateException("Could not find any key owned by " + primaryOwner);

      log.infof("Binary key %s hashes to [cluster=%s,hotrod=%s]",
            Util.printArray(dummy, false), primaryOwner.getCacheManager().getAddress(),
            primaryOwner.getAddress());

      return dummy;
   }

}
