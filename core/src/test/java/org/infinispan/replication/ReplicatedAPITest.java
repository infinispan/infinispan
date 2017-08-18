package org.infinispan.replication;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.testng.AssertJUnit.assertEquals;

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
      build.clustering().stateTransfer().timeout(10000);
      createClusteredCaches(2, "replication", build);
   }

   public void put() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      // test a simple put!
      assert cache1.get("key") == null;
      assert cache2.get("key") == null;

      cache1.put("key", "value");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      Map<String, String> map = new HashMap<>();
      map.put("key2", "value2");
      map.put("key3", "value3");

      cache1.putAll(map);

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");
      assert cache1.get("key2").equals("value2");
      assert cache2.get("key2").equals("value2");
      assert cache1.get("key3").equals("value3");
      assert cache2.get("key3").equals("value3");
   }

   public void remove() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assert cache2.get("key").equals("value");
      assert cache1.get("key") == null;

      cache1.remove("key");

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value");
      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value");

      cache1.remove("key");

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testPutIfAbsent() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "valueOld");
      assert cache2.get("key").equals("valueOld");
      assert cache1.get("key") == null;

      cache1.putIfAbsent("key", "value");

      assertEquals("value", cache1.get("key"));
      assertEquals("value", cache2.get("key"));

      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value3");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value3");

      cache1.putIfAbsent("key", "value4");

      assert cache1.get("key").equals("value");
      assert cache2.get("key").equals("value3"); // should not invalidate cache2!!
   }

   public void testRemoveIfPresent() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.remove("key", "value");

      assert cache1.get("key").equals("value1") : "Should not remove";
      assert cache2.get("key").equals("value2") : "Should not remove";

      cache1.remove("key", "value1");

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testClear() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.clear();

      assert cache1.get("key") == null;
      assert cache2.get("key") == null;
   }

   public void testReplace() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN");

      cache1.replace("key", "value1");

      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value1");
   }

   public void testReplaceWithOldVal() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key") == null;
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "valueN");

      cache1.replace("key", "valueOld", "value1"); // should do nothing since there is nothing to replace on cache1

      assert cache1.get("key").equals("valueN");
      assert cache2.get("key").equals("value2");

      cache1.replace("key", "valueN", "value1");

      // the replace executed identically on both of them
      assertEquals("value1", cache1.get("key"));
      assertEquals("value1", cache2.get("key"));
   }

   public void testLocalOnlyClear() {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      cache1.withFlags(CACHE_MODE_LOCAL).put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      cache1.withFlags(CACHE_MODE_LOCAL).clear();

      assert cache1.get("key") == null;
      assert cache2.get("key") != null;
      assert cache2.get("key").equals("value2");
   }
}
