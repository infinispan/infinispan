package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.retry.DistributionRetryTest;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.eventually.Eventually;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 */
@Test (groups = "functional", testName = "client.hotrod.ConsistentHashV2IntegrationTest")
public class ConsistentHashV2IntegrationTest extends MultipleCacheManagersTest {
   public static final int NUM_KEYS = 200;

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;
   private HotRodServer hotRodServer4; //tod add shutdown behaviour
   private RemoteCacheManager remoteCacheManager;
   private RemoteCacheImpl remoteCache;
   private KeyAffinityService kas;
   private ExecutorService ex;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = buildConfiguration();

      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(manager(0));
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(manager(1));
      hotRodServer3 = HotRodClientTestingUtil.startHotRodServer(manager(2));
      hotRodServer4 = HotRodClientTestingUtil.startHotRodServer(manager(3));


      waitForClusterToForm();

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotRodServer2.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = (RemoteCacheImpl) remoteCacheManager.getCache();
      assert super.cacheManagers.size() == 4;

      ex = Executors.newSingleThreadExecutor(getTestThreadFactory("KeyGenerator"));
      kas = KeyAffinityServiceFactory.newKeyAffinityService(cache(0),
            ex, new DistributionRetryTest.ByteKeyGenerator(), 2, true);

      for (int i = 0; i < 4; i++) {
         advancedCache(i).getAsyncInterceptorChain()
                         .addInterceptor(new HitsAwareCacheManagersTest.HitCountInterceptor(), 1);
      }
   }

   private ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.jmxStatistics().enable();
      builder.clustering().hash().numOwners(2).stateTransfer().fetchInMemoryState(false);
      return hotRodCacheConfiguration(builder);
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   @AfterTest
   public void cleanUp() {
      ex.shutdownNow();
      kas.stop();

      stopServer(hotRodServer1);
      stopServer(hotRodServer2);
      stopServer(hotRodServer3);
      stopServer(hotRodServer4);

      remoteCache.stop();
      remoteCacheManager.stop();
   }

   private void stopServer(HotRodServer hrs) {
      killServers(hrs);
   }

   public void testCorrectBalancingOfKeys() {
      runTest(0);
      runTest(1);
      runTest(2);
      runTest(3);
   }

   private void runTest(int cacheIndex) {
      LocalizedCacheTopology serverTopology = advancedCache(cacheIndex).getDistributionManager().getCacheTopology();

      for (int i = 0; i < NUM_KEYS; i++) {
         byte[] keyBytes = (byte[]) kas.getKeyForAddress(address(cacheIndex));
         String key = DistributionRetryTest.ByteKeyGenerator.getStringObject(keyBytes);
         Address serverPrimary = serverTopology.getDistribution(keyBytes).primary();
         assertEquals(address(cacheIndex), serverPrimary);

         remoteCache.put(key, "v");
      }

      // compatibility with 1.0/1.1 clients is not perfect, so we must allow for some misses
      assertTrue(hitCountInterceptor(cacheIndex).getHits() > NUM_KEYS * 0.99);
      hitCountInterceptor(cacheIndex).reset();
   }

   public void testCorrectBalancingOfKeysAfterNodeKill() {
      //final AtomicInteger clientTopologyId = TestingUtil.extractField(remoteCacheManager, "defaultCacheTopologyId");
      TcpTransportFactory transportFactory = TestingUtil.extractField(remoteCacheManager, "transportFactory");

      final int topologyIdBeforeJoin = transportFactory.getTopologyId(new byte[]{});
      log.tracef("Starting test with client topology id %d", topologyIdBeforeJoin);
      EmbeddedCacheManager cm5 = addClusterEnabledCacheManager(buildConfiguration());
      HotRodServer hotRodServer5 = HotRodClientTestingUtil.startHotRodServer(cm5);

      // Rebalancing to include the joiner will increment the topology id by 2
      Eventually.eventually(() -> {
         int topologyId = transportFactory.getTopologyId(new byte[]{});
         log.tracef("Client topology id is %d, waiting for it to become %d", topologyId,
               topologyIdBeforeJoin + 2);
         // The put operation will update the client topology (if necessary)
         remoteCache.put("k", "v");
         return topologyId >= topologyIdBeforeJoin + 2;
      });

      resetHitInterceptors();
      runTest(0);
      runTest(1);
      runTest(2);
      runTest(3);

      stopServer(hotRodServer5);
      TestingUtil.killCacheManagers(cm5);

      // Rebalancing to exclude the leaver will again increment the topology id by 2
      Eventually.eventually(() -> {
         int topologyId = transportFactory.getTopologyId(new byte[]{});
         log.tracef("Client topology id is %d, waiting for it to become %d", topologyId,
               topologyIdBeforeJoin + 4);
         // The put operation will update the client topology (if necessary)
         remoteCache.put("k", "v");
         return topologyId >= topologyIdBeforeJoin + 4;
      });

      resetHitInterceptors();
      runTest(0);
      runTest(1);
      runTest(2);
      runTest(3);
   }

  private void resetHitInterceptors() {
      for (int i = 0; i < 4; i++) {
         HitsAwareCacheManagersTest.HitCountInterceptor interceptor = hitCountInterceptor(i);
         interceptor.reset();
      }
   }

   private HitsAwareCacheManagersTest.HitCountInterceptor hitCountInterceptor(int i) {
      AsyncInterceptorChain ic = advancedCache(i).getAsyncInterceptorChain();
      return ic.findInterceptorWithClass(HitsAwareCacheManagersTest.HitCountInterceptor.class);
   }
}
