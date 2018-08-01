package org.infinispan.client.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.ObjectStorageRoutingTest", groups = "functional")
public class ObjectStorageRoutingTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1)
      .encoding().key().mediaType(APPLICATION_OBJECT_TYPE)
      .encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      builder.jmxStatistics().enable();
      return builder;
   }

   public void testGetWithObjectStorage() {
      RemoteCache<String, String> client = client(0).getCache();
      HashMap<String, String> cachedValues = new HashMap<>();
      for (int i = 0; i < 100; i++) {
         String key = String.format("key-%d", i);
         String value = String.format("value-%d", i);
         client.put(key, value);
         cachedValues.put(key, value);
      }

      int startRpcs = 0;
      for (Cache cache : caches()) {
         startRpcs += ((RpcManagerImpl) cache.getAdvancedCache().getRpcManager()).getReplicationCount();
      }
      assertTrue(startRpcs > 0);
      for (Map.Entry<String, String> entry : cachedValues.entrySet()) {
         String value = client.get(entry.getKey());
         assertEquals(entry.getValue(), value);
      }
      int endRpcs = 0;
      for (Cache cache : caches()) {
         endRpcs += ((RpcManagerImpl) cache.getAdvancedCache().getRpcManager()).getReplicationCount();
      }
      assertEquals(startRpcs, endRpcs);
   }


}
