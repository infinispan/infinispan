package org.infinispan.api.flags;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.SKIP_CACHE_STORE;
import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingAdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.persistence.UnnecessaryLoadingTest;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "api.flags.FlagsEnabledTest")
@CleanupAfterMethod
public class FlagsEnabledTest extends MultipleCacheManagersTest {

   protected final String cacheName;

   public FlagsEnabledTest() {
      this("tx-replication");
   }

   protected FlagsEnabledTest(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder
            .locking().writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ)
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .persistence().addStore(UnnecessaryLoadingTest.CountingStoreConfigurationBuilder.class)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .transaction().syncCommitPhase(true)
            .clustering().hash().numSegments(2);
      createClusteredCaches(2, cacheName, builder);
   }

   UnnecessaryLoadingTest.CountingStore getCacheStore(Cache cache) {
      return (UnnecessaryLoadingTest.CountingStore) TestingUtil.getFirstLoader(cache);
   }

   public void testWithFlagsSemantics() {
      final AdvancedCache<String, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<String, String> cache2 = advancedCache(1, cacheName);

      assertNumberOfLoads(cache1, 0);
      assertNumberOfLoads(cache2, 0);

      final AdvancedCache<String, String> cache1LocalOnly = cache1.withFlags(CACHE_MODE_LOCAL);
      cache1LocalOnly.put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assertCacheValue(cache1, "key", "value1");
      assertCacheValue(cache2, "key", "value2");

      assertNumberOfLoads(cache1, 1);
      assertNumberOfLoads(cache2, 1);
      assertNotSame("CacheStores", getCacheStore(cache1), getCacheStore(cache2));

      cache1.put("nonLocal", "value");
      assertCacheValue(cache2, "nonLocal", "value");
      assertNumberOfLoads(cache1, 2);
      assertNumberOfLoads(cache2, 1); //not incremented since ISPN-1642

      final AdvancedCache<String, String> cache1SkipRemoteAndStores = cache1LocalOnly.withFlags(SKIP_CACHE_STORE);
      cache1SkipRemoteAndStores.put("again", "value");
      assertNumberOfLoads(cache1, 2);
      assertNumberOfLoads(cache2, 1);
      assertCacheValue(cache1, "again", "value");
      assertCacheValue(cache2, "again", null);

      assertNumberOfLoads(cache1, 2);
      assertNumberOfLoads(cache2, 2); //"again" wasn't found in cache, looks into store

      assertCacheValue(cache2, "again", null);
      assertNumberOfLoads(cache2, 3);
      assertCacheValue(cache2.withFlags(SKIP_CACHE_STORE), "again", null);
      assertNumberOfLoads(cache2, 3);

      assertNumberOfLoads(cache1, 2);
      assertCacheValue(cache1LocalOnly, "localStored", null);
      assertNumberOfLoads(cache1, 3); //options on cache1SkipRemoteAndStores did NOT affect this cache
   }

   public void testWithFlagsAndDelegateCache() {
      final AdvancedCache<Integer, String> c1 =
            new CustomDelegateCache<Integer, String>(this.<Integer, String>advancedCache(0, cacheName));
      final AdvancedCache<Integer, String> c2 = advancedCache(1, cacheName);

      c1.withFlags(CACHE_MODE_LOCAL).put(1, "v1");
      assertCacheValue(c2, 1, null);
   }

   public void testReplicateSkipCacheLoad(Method m) {
      final AdvancedCache<String, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<String, String> cache2 = advancedCache(1, cacheName);
      assertNumberOfLoads(cache1, 0);
      assertNumberOfLoads(cache2, 0);

      final String v = v(m, 1);
      final String k = k(m, 1);
      cache1.withFlags(Flag.SKIP_CACHE_LOAD).put(k, v);
      assertCacheValue(cache2, k, v);

      assertNumberOfLoads(cache1, 0);
      assertNumberOfLoads(cache2, 0);
   }

   public void testReplicateSkipCacheLoaderWithinTxInCoordinator(Method m) throws Exception {
      final AdvancedCache<String, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<String, String> cache2 = advancedCache(1, cacheName);
      doReplicateSkipCacheLoaderWithinTx(m, cache1, cache2);
   }

   public void testReplicateSkipCacheLoaderWithinTxInNonCoordinator(Method m) throws Exception {
      final AdvancedCache<String, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<String, String> cache2 = advancedCache(1, cacheName);
      doReplicateSkipCacheLoaderWithinTx(m, cache2, cache1);
   }

   public void testCacheLocalInPrimaryOwner() {
      final AdvancedCache<Object, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<Object, String> cache2 = advancedCache(1, cacheName);
      final Object key = new MagicKey("k-po", cache1);

      cache1.withFlags(CACHE_MODE_LOCAL).put(key, "value");

      assertCacheValue(cache1, key, "value");
      assertCacheValue(cache2, key, null);
   }

   public void testCacheLocalInBackupOwner() {
      final AdvancedCache<Object, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<Object, String> cache2 = advancedCache(1, cacheName);
      final Object key = new MagicKey("k-bo", cache1);

      cache2.withFlags(CACHE_MODE_LOCAL).put(key, "value");

      assertCacheValue(cache2, key, "value");
      assertCacheValue(cache1, key, null);
   }

   private void doReplicateSkipCacheLoaderWithinTx(Method m,
         final AdvancedCache<String, String> cache1,
         AdvancedCache<String, String> cache2) throws Exception {
      assertNumberOfLoads(cache1, 0);
      assertNumberOfLoads(cache2, 0);

      final String v = v(m, 1);
      final String k = k(m, 1);
      withTx(cache1.getTransactionManager(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache1.withFlags(Flag.SKIP_CACHE_LOAD).put(k, v);
            return null;
         }
      });

      assertCacheValue(cache2, k, v);
      assertNumberOfLoads(cache1, 0);
      assertNumberOfLoads(cache2, 0);
   }

   public static class CustomDelegateCache<K, V>
         extends AbstractDelegatingAdvancedCache<K, V> {

      public CustomDelegateCache(AdvancedCache<K, V> cache) {
         super(cache, new AdvancedCacheWrapper<K, V>() {
            @Override
            public AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache) {
               return new CustomDelegateCache<K, V>(cache);
            }
         });
      }
   }

   private void assertNumberOfLoads(Cache<?, ?> cache, int expectedCounter) {
      assertEquals("Wrong number of loads for cache '" + cache + "'.", expectedCounter, getCacheStore(cache).numLoads);
   }

   protected final void assertCacheValue(Cache<?, ?> cache, Object key, Object value) {
      assertEquals("Wrong value for key '" + key + "' in cache '" + cache + "'.", value, cache.get(key));
   }

}
