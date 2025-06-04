package org.infinispan.client.hotrod;

import static org.infinispan.configuration.cache.StorageType.HEAP;
import static org.infinispan.configuration.cache.StorageType.OFF_HEAP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.test.FixedServerBalancing;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

/**
 * Tests that the Hot Rod client can correctly route requests to a server using different {@link StorageType}.
 *
 * @since 11.0
 */
@Test(groups = "functional", testName = "client.hotrod.StorageRoutingTest")
public class StorageRoutingTest extends MultiHotRodServersTest {

   private static final int CLUSTER_SIZE = 3;

   private Object key;

   public Object[] factory() {
      String stringKey = "key";
      byte[] byteArrayKey = new byte[]{1, 2, 3};

      return new Object[]{
            new StorageRoutingTest().withStorageType(HEAP).withKey(stringKey),
            new StorageRoutingTest().withStorageType(HEAP).withKey(byteArrayKey),
            new StorageRoutingTest().withStorageType(OFF_HEAP).withKey(stringKey),
            new StorageRoutingTest().withStorageType(OFF_HEAP).withKey(byteArrayKey),
      };
   }

   protected String[] parameterNames() {
      return new String[]{null, "key"};
   }

   protected Object[] parameterValues() {
      return new Object[]{storageType, key.getClass().getSimpleName()};
   }

   private StorageRoutingTest withStorageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   private StorageRoutingTest withKey(Object key) {
      this.key = key;
      return this;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfgBuilder.statistics().enable();
      cfgBuilder.clustering().hash().numOwners(1);
      cfgBuilder.memory().storage(storageType);
      createHotRodServers(CLUSTER_SIZE, cfgBuilder);
      waitForClusterToForm();
   }

   @Override
   protected void modifyGlobalConfiguration(GlobalConfigurationBuilder builder) {
      super.modifyGlobalConfiguration(builder);
      builder.metrics().accurateSize(true);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(HotRodServer hotRodServer) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder hotRodClientConfigurationBuilder = super.createHotRodClientConfigurationBuilder(hotRodServer);
      hotRodClientConfigurationBuilder.balancingStrategy(() -> new FixedServerBalancing(hotRodServer));
      return hotRodClientConfigurationBuilder;
   }

   @Test
   public void shouldContactKeyOwnerForPutGet() {
      String value = "value";
      RemoteCache<Object, String> remoteCache = clients.get(0).getCache();
      remoteCache.put(key, value);

      assertEquals(remoteCache.get(key), "value");

      assertCorrectServerContacted();
   }

   private void assertCorrectServerContacted() {
      AtomicInteger storedIn = new AtomicInteger(-1);
      AtomicInteger retrievedFrom = new AtomicInteger(-1);
      for (int i = 0; i < clients.size(); i++) {
         RemoteCacheManager rcm = client(i);
         RemoteCache<?, ?> cache = rcm.getCache();
         ServerStatistics statistics = cache.serverStatistics();
         int retrievals = statistics.getIntStatistic("retrievals");
         int dataContainerSize = statistics.getIntStatistic("currentNumberOfEntries");
         if (retrievals == 1) {
            if (!retrievedFrom.compareAndSet(-1, i)) fail("Retrieval happened in more than 1 server!");
         }
         if (dataContainerSize == 1) {
            if (!storedIn.compareAndSet(-1, i)) fail("Store happened in more than 1 server!");
         }
      }
      int storeServer = storedIn.get();
      int retrieveServer = retrievedFrom.get();
      assertTrue(storeServer != -1, "Entry was not stored!");
      assertTrue(retrieveServer != -1, "Entry was not retrieved!");
      assertEquals(storeServer, retrieveServer, String.format("Entry stored on server %d but retrieved from server %d", storeServer, retrieveServer));
   }
}
