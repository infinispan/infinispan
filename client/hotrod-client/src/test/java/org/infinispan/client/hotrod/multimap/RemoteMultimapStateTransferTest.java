package org.infinispan.client.hotrod.multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * State transfer test for multimap to ensure the Bucket class is properly sent.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "client.hotrod.multimap.RemoteMultimapStateTransferTest")
public class RemoteMultimapStateTransferTest extends MultiHotRodServersTest {

   private static final int NODES = 2;
   private static final int VALUES = 4;
   private static final String CACHE_NAME = "multimap";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createHotRodServers(NODES, new ConfigurationBuilder());
      defineInAll(CACHE_NAME, cacheBuilder);
   }

   public void testStateTransfer() {
      RemoteMultimapCache<String, String> mc = multimapCache(0);

      List<String> values1 = createValues();
      String key1 = UUID.randomUUID().toString();

      storeValues(mc, key1, values1);

      List<String> values2 = createValues();
      String key2 = UUID.randomUUID().toString();

      storeValues(mc, key2, values2);

      for (int i = 0; i < NODES; ++i) {
         assertData(i, key1, values1);
         assertData(i, key2, values2);
      }

      HotRodServer server = addHotRodServerAndClient(new ConfigurationBuilder());
      defineCache(server, CACHE_NAME, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));

      for (int i = 0; i < NODES + 1; ++i) {
         assertData(i, key1, values1);
         assertData(i, key2, values2);
      }
   }

   private RemoteMultimapCache<String, String> multimapCache(int index) {
      MultimapCacheManager<String, String> mcm = RemoteMultimapCacheManagerFactory.from(client(index));
      return mcm.get(CACHE_NAME);
   }

   private void assertData(int index, String key, List<String> values) {
      RemoteMultimapCache<String, String> mc = multimapCache(index);
      Collection<String> data = mc.get(key).join();
      AssertJUnit.assertEquals(values.size(), data.size());
      for (String v : values) {
         AssertJUnit.assertTrue(data.contains(v));
      }
   }

   private static void storeValues(RemoteMultimapCache<String, String> rmc, String key, List<String> values) {
      for (String v : values) {
         rmc.put(key, v);
      }
   }

   private static List<String> createValues() {
      List<String> values = new ArrayList<>(VALUES);
      for (int i = 0; i < VALUES; ++i) {
         values.add(UUID.randomUUID().toString());
      }
      return values;
   }

}
