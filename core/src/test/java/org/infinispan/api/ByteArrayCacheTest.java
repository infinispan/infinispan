package org.infinispan.api;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;


/**
 * Test that verifies that when custom, or JDK, objects that have undesirable
 * equality checks, i.e. byte arrays, are stored in the cache, then the
 * correct results are returned with different configurations (with or
 * without key/value equivalence set up).
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "api.ByteArrayCacheTest")
@CleanupAfterMethod
public class ByteArrayCacheTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // If key equivalence is set, it will also be used for value
      builder.dataContainer().keyEquivalence(ByteArrayEquivalence.INSTANCE);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testByteArrayValueOnlyReplace() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer().valueEquivalence(ByteArrayEquivalence.INSTANCE);
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            // Mimics Memcached/REST endpoints where only value side is byte array
            Cache<Integer, byte[]> cache = cm.getCache();
            final Integer key = 2;
            final byte[] value = {1, 2, 3};
            cache.put(key, value);
            // Use a different instance deliberately
            final byte[] oldValue = {1, 2, 3};
            final byte[] newValue = {4, 5, 6};
            assertTrue(cache.replace(key, oldValue, newValue));
         }
      });
   }

   public void testByteArrayGet() {
      Map<byte[], byte[]> map = cache();
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] lookupKey = {1, 2, 3}; // on purpose, different instance required
      assertTrue(String.format("Expected key=%s to return value=%s",
            Util.toStr(lookupKey), Util.toStr(value)),
            Arrays.equals(value, map.get(lookupKey)));
   }
}
