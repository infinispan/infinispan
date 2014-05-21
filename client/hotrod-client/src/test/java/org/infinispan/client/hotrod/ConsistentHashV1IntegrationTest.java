package org.infinispan.client.hotrod;

import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.retry.DistributionRetryTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 */
@Test (groups = "functional", testName = "client.hotrod.ConsistentHashV1IntegrationTest")
public class ConsistentHashV1IntegrationTest extends MultipleCacheManagersTest {
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

      hotRodServer1 = TestHelper.startHotRodServer(manager(0));
      hotRodServer2 = TestHelper.startHotRodServer(manager(1));
      hotRodServer3 = TestHelper.startHotRodServer(manager(2));
      hotRodServer4 = TestHelper.startHotRodServer(manager(3));


      waitForClusterToForm();

      Properties clientConfig = new Properties();
      clientConfig.put("infinispan.client.hotrod.server_list", "localhost:" + hotRodServer2.getPort());

      remoteCacheManager = new RemoteCacheManager(clientConfig);
      remoteCache = (RemoteCacheImpl) remoteCacheManager.getCache();
      assert super.cacheManagers.size() == 4;

      ex = Executors.newSingleThreadExecutor();
      kas = KeyAffinityServiceFactory.newKeyAffinityService(cache(0),
            ex, new DistributionRetryTest.ByteKeyGenerator(), 2, true);

      for (int i = 0; i < 4; i++) {
         advancedCache(i).addInterceptor(new HitsAwareCacheManagersTest.HitCountInterceptor(), 1);
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
      ConsistentHash serverCH = advancedCache(cacheIndex).getDistributionManager().getConsistentHash();

      for (int i = 0; i < NUM_KEYS; i++) {
         byte[] keyBytes = (byte[]) kas.getKeyForAddress(address(cacheIndex));
         String key = DistributionRetryTest.ByteKeyGenerator.getStringObject(keyBytes);
         Address serverPrimary = serverCH.locatePrimaryOwner(keyBytes);
         assertEquals(address(cacheIndex), serverPrimary);

         remoteCache.put(key, "v");
      }

      // compatibility with 1.0/1.1 clients is not perfect, so we must allow for some misses
      assertTrue(hitCountInterceptor(cacheIndex).getHits() > NUM_KEYS * 0.99);
      hitCountInterceptor(cacheIndex).reset();
   }

   public void testCorrectBalancingOfKeysAfterNodeKill() {
      final AtomicInteger clientTopologyId = TestingUtil.extractField(remoteCacheManager, "defaultCacheTopologyId");

      final int topologyIdBeforeJoin = clientTopologyId.get();
      log.tracef("Starting test with client topology id %d", topologyIdBeforeJoin);
      EmbeddedCacheManager cm5 = addClusterEnabledCacheManager(buildConfiguration());
      HotRodServer hotRodServer5 = TestHelper.startHotRodServer(cm5);

      // Rebalancing to include the joiner will increment the topology id by 2
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            log.tracef("Client topology id is %d, waiting for it to become %d", clientTopologyId.get(),
                  topologyIdBeforeJoin + 2);
            // The put operation will update the client topology (if necessary)
            remoteCache.put("k", "v");
            return clientTopologyId.get() >= topologyIdBeforeJoin + 2;
         }
      });

      resetHitInterceptors();
      runTest(0);
      runTest(1);
      runTest(2);
      runTest(3);

      stopServer(hotRodServer5);
      TestingUtil.killCacheManagers(cm5);

      // Rebalancing to exclude the leaver will again increment the topology id by 2
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            log.tracef("Client topology id is %d, waiting for it to become %d", clientTopologyId.get(),
                  topologyIdBeforeJoin + 4);
            // The put operation will update the client topology (if necessary)
            remoteCache.put("k", "v");
            return clientTopologyId.get() >= topologyIdBeforeJoin + 4;
         }
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
      InterceptorChain ic = advancedCache(i).getComponentRegistry().getComponent(InterceptorChain.class);
      return (HitsAwareCacheManagersTest.HitCountInterceptor) ic.getInterceptorsWithClass(HitsAwareCacheManagersTest.HitCountInterceptor.class).get(0);
   }
}
