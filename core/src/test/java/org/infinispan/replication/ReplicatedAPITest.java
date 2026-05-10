package org.infinispan.replication;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "replication.ReplicatedAPITest")
public class ReplicatedAPITest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder build = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      build.clustering().stateTransfer().timeout(30000);
      createClusteredCaches(2, "replication", build);
   }

   public void put() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      // test a simple put!
      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));

      cache1.put("key", "value");

      assertEquals("value", cache1.get("key"));
      assertEquals("value", cache2.get("key"));

      Map<String, String> map = new HashMap<>();
      map.put("key2", "value2");
      map.put("key3", "value3");

      cache1.putAll(map);

      assertEquals("value", cache1.get("key"));
      assertEquals("value", cache2.get("key"));
      assertEquals("value2", cache1.get("key2"));
      assertEquals("value2", cache2.get("key2"));
      assertEquals("value3", cache1.get("key3"));
      assertEquals("value3", cache2.get("key3"));
   }

   public void remove() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assertEquals("value", cache2.get("key"));
      assertNull(cache1.get("key"));

      cache1.remove("key");

      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assertEquals("value", cache1.get("key"));
      assertEquals("value", cache2.get("key"));

      cache1.remove("key");

      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));
   }

   public void testPutIfAbsent() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "valueOld");
      assertEquals("valueOld", cache2.get("key"));
      assertNull(cache1.get("key"));

      cache1.putIfAbsent("key", "value");

      assertEquals("value", cache1.get("key"));
      assertEquals("value", cache2.get("key"));

      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value3");

      assertEquals("value", cache1.get("key"));
      assertEquals("value3", cache2.get("key"));

      cache1.putIfAbsent("key", "value4");

      assertEquals("value", cache1.get("key"));
      assertEquals("value3", cache2.get("key")); // should not invalidate cache2!!
   }

   public void testRemoveIfPresent() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertEquals("value1", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.remove("key", "value");

      assertEquals("value1", cache1.get("key"), "Should not remove");
      assertEquals("value2", cache2.get("key"), "Should not remove");

      cache1.remove("key", "value1");

      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));
   }

   public void testClear() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertEquals("value1", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.clear();

      assertNull(cache1.get("key"));
      assertNull(cache2.get("key"));
   }

   public void testReplace() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertNull(cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.replace("key", "value1"); // should do nothing since there is nothing to replace on cache1

      assertNull(cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN");

      cache1.replace("key", "value1");

      assertEquals("value1", cache1.get("key"));
      assertEquals("value1", cache2.get("key"));
   }

   public void testReplaceWithOldVal() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertNull(cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assertNull(cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN");

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assertEquals("valueN", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.replace("key", "valueN", "value1");

      // the replace executed identically on both of them
      assertEquals("value1", cache1.get("key"));
      assertEquals("value1", cache2.get("key"));
   }

   public void testLocalOnlyClear() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertEquals("value1", cache1.get("key"));
      assertEquals("value2", cache2.get("key"));

      cache1.withFlags(CACHE_MODE_LOCAL).clear();

      assertNull(cache1.get("key"));
      assertNotNull(cache2.get("key"));
      assertEquals("value2", cache2.get("key"));
   }
}
