package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.testng.annotations.Test;

/**
 * Tests that IGNORE_RETURN_VALUES flag prevents returning values when replacing existing entries.
 */
@Test(groups = "functional", testName = "api.IgnoreReturnValuesTest")
public class IgnoreReturnValuesTest extends MultipleCacheManagersTest {

   @Override
   public Object[] factory() {
      return new Object[] {
         new IgnoreReturnValuesTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
         new IgnoreReturnValuesTest().cacheMode(CacheMode.DIST_SYNC).transactional(true),
         new IgnoreReturnValuesTest().cacheMode(CacheMode.LOCAL).transactional(false),
         new IgnoreReturnValuesTest().cacheMode(CacheMode.LOCAL).transactional(true),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(cacheMode, transactional);
      createCluster(TestDataSCI.INSTANCE, dcc, 2);
      waitForClusterToForm();
   }

   public void testPutReplace() {
      Object k = cacheMode.isDistributed() ? getKeyForCache(0) : "key1";
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));

      // Without flag, put should return old value
      Object oldValue = cache(0).put(k, "v1");
      assertEquals("v0", oldValue);
      assertEquals("v1", cache(0).get(k));

      // With IGNORE_RETURN_VALUES flag, put should return null
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object result = cache.put(k, "v2");
      assertNull("put with IGNORE_RETURN_VALUES should return null", result);
      assertEquals("v2", cache(0).get(k));
   }

   public void testRemove() {
      Object k = cacheMode.isDistributed() ? getKeyForCache(0) : "key2";
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));

      // Without flag, remove should return old value
      Object oldValue = cache(0).remove(k);
      assertEquals("v0", oldValue);
      assertNull(cache(0).get(k));

      // With IGNORE_RETURN_VALUES flag, remove should return null
      cache(0).put(k, "v1");
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object result = cache.remove(k);
      assertNull("remove with IGNORE_RETURN_VALUES should return null", result);
      assertNull(cache(0).get(k));
   }

   public void testReplace() {
      Object k = cacheMode.isDistributed() ? getKeyForCache(0) : "key3";
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));

      // Without flag, replace should return old value
      Object oldValue = cache(0).replace(k, "v1");
      assertEquals("v0", oldValue);
      assertEquals("v1", cache(0).get(k));

      // With IGNORE_RETURN_VALUES flag, replace should return null
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object result = cache.replace(k, "v2");
      assertNull("replace with IGNORE_RETURN_VALUES should return null", result);
      assertEquals("v2", cache(0).get(k));
   }

   public void testCompute() {
      Object k = cacheMode.isDistributed() ? getKeyForCache(0) : "key4";
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));

      // Without flag, compute should return new value
      Object newValue = cache(0).compute(k, (key, value) -> value + "-modified");
      assertEquals("v0-modified", newValue);
      assertEquals("v0-modified", cache(0).get(k));

      // With IGNORE_RETURN_VALUES flag, compute should return null
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object result = cache.compute(k, (key, value) -> value + "-again");
      assertNull("compute with IGNORE_RETURN_VALUES should return null", result);
      assertEquals("v0-modified-again", cache(0).get(k));
   }

   public void testComputeIfPresent() {
      Object k = cacheMode.isDistributed() ? getKeyForCache(0) : "key5";
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));

      // Without flag, computeIfPresent should return new value
      Object newValue = cache(0).computeIfPresent(k, (key, value) -> value + "-modified");
      assertEquals("v0-modified", newValue);
      assertEquals("v0-modified", cache(0).get(k));

      // With IGNORE_RETURN_VALUES flag, computeIfPresent should return null
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object result = cache.computeIfPresent(k, (key, value) -> value + "-again");
      assertNull("computeIfPresent with IGNORE_RETURN_VALUES should return null", result);
      assertEquals("v0-modified-again", cache(0).get(k));
   }

   public void testMerge() {
      Object k = cacheMode.isDistributed() ? getKeyForCache(0) : "key6";
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));

      // Without flag, merge should return new value
      Object newValue = cache(0).merge(k, "-added", (oldVal, newVal) -> String.valueOf(oldVal) + String.valueOf(newVal));
      assertEquals("v0-added", newValue);
      assertEquals("v0-added", cache(0).get(k));

      // With IGNORE_RETURN_VALUES flag, merge should return null
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object result = cache.merge(k, "-more", (oldVal, newVal) -> String.valueOf(oldVal) + String.valueOf(newVal));
      assertNull("merge with IGNORE_RETURN_VALUES should return null", result);
      assertEquals("v0-added-more", cache(0).get(k));
   }

   public void testPutIfAbsent() {
      Object k = cacheMode.isDistributed() ? getKeyForCache(0) : "key7";
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));

      // Without flag, putIfAbsent on existing key should return existing value
      Object existingValue = cache(0).putIfAbsent(k, "v1");
      assertEquals("v0", existingValue);
      assertEquals("v0", cache(0).get(k));

      // With IGNORE_RETURN_VALUES flag, putIfAbsent should return null
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object result = cache.putIfAbsent(k, "v2");
      assertNull("putIfAbsent with IGNORE_RETURN_VALUES should return null", result);
      // Value should not change since key already exists
      assertEquals("v0", cache(0).get(k));
   }

   public void testComputeIfAbsent() {
      Object k = cacheMode.isDistributed() ? getKeyForCache(0) : "key8";
      cache(0).put(k, "v0");
      assertEquals("v0", cache(0).get(k));

      // Without flag, computeIfAbsent on existing key should return existing value
      Object existingValue = cache(0).computeIfAbsent(k, key -> "v1");
      assertEquals("v0", existingValue);
      assertEquals("v0", cache(0).get(k));

      // With IGNORE_RETURN_VALUES flag, computeIfAbsent should return null
      AdvancedCache<Object, Object> cache = advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES);
      Object result = cache.computeIfAbsent(k, key -> "v2");
      assertNull("computeIfAbsent with IGNORE_RETURN_VALUES should return null", result);
      // Value should not change since key already exists
      assertEquals("v0", cache(0).get(k));
   }
}
