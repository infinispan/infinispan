package org.infinispan.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.SimpleConditionalOperationTest")
public class SimpleConditionalOperationTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(TestDataSCI.INSTANCE, getConfig(), 2);
      waitForClusterToForm();
   }

   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   public void testReplaceFromMainOwner() throws Throwable {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      tm(0).begin();
      cache(0).put("kkk", "vvv");
      cache(0).replace(k, "v1", "v2");
      tm(0).commit();

      assertEquals("0", advancedCache(0).getDataContainer().peek(k).getValue());
      assertEquals("0", advancedCache(1).getDataContainer().peek(k).getValue());

      log.trace("here is the interesting replace.");
      cache(0).replace(k, "0", "1");
      assertEquals("1", advancedCache(0).getDataContainer().peek(k).getValue());
      assertEquals("1", advancedCache(1).getDataContainer().peek(k).getValue());
   }

   public void testRemoveFromMainOwner() {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      cache(0).remove(k, "1");

      assertEquals("0", advancedCache(0).getDataContainer().peek(k).getValue());
      assertEquals("0", advancedCache(1).getDataContainer().peek(k).getValue());

      cache(0).remove(k, "0");
      assertNull(advancedCache(0).getDataContainer().peek(k));
      assertNull(advancedCache(1).getDataContainer().peek(k));
   }

   public void testPutIfAbsentFromMainOwner() {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      cache(0).putIfAbsent(k, "1");

      assertEquals("0", advancedCache(0).getDataContainer().peek(k).getValue());
      assertEquals("0", advancedCache(1).getDataContainer().peek(k).getValue());

      cache(0).remove(k);

      cache(0).putIfAbsent(k, "1");
      assertEquals("1", advancedCache(0).getDataContainer().peek(k).getValue());
      assertEquals("1", advancedCache(1).getDataContainer().peek(k).getValue());
   }
}
