package org.infinispan.client.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.GetAllObjectStorageDistTest", groups = "functional")
public class GetAllObjectStorageDistTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1)
            .encoding().key().mediaType(APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      return builder;
   }

   public void testGetAllWithObjectStorage() {
      RemoteCache<String, String> cache = client(0).getCache();
      HashMap<String, String> cachedValues = new HashMap<>();
      for(int i=0; i<100; i++){
         String key = String.format("key-%d", i);
         String value = String.format("value-%d", i);
         cache.put(key, value);
         cachedValues.put(key, value);
      }

      Map<String, String> values = cache.getAll(cachedValues.keySet());
      assertEquals(cachedValues.size(), values.size());
      for(String key : values.keySet()){
         assertEquals(cachedValues.get(key), values.get(key));
      }
   }

}
