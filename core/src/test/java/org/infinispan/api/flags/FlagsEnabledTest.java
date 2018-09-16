package org.infinispan.api.flags;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.SKIP_CACHE_LOAD;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.withTx;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingAdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
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
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      createClusteredCaches(2, cacheName, builder);
   }

   DummyInMemoryStore getCacheStore(Cache cache) {
      return (DummyInMemoryStore) TestingUtil.getFirstLoader(cache);
   }

   public void testWithFlagsSemantics() {
      final AdvancedCache<MagicKey, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<MagicKey, String> cache2 = advancedCache(1, cacheName);
      assertNotSame("CacheStores", getCacheStore(cache1), getCacheStore(cache2));
      assertLoadsAndReset(cache1, 0, cache2, 0);

      final AdvancedCache<MagicKey, String> cache1LocalOnly = cache1.withFlags(CACHE_MODE_LOCAL);
      MagicKey localKey = new MagicKey("local", cache1);
      cache1LocalOnly.put(localKey, "value1");
      assertLoadsAndReset(cache1, 1, cache2, 0);

      cache2.withFlags(CACHE_MODE_LOCAL).put(localKey, "value2");
      assertLoadsAndReset(cache1, 0, cache2, 1);

      assertCacheValue(cache1, localKey, "value1");
      assertLoadsAndReset(cache1, 0, cache2, 0);

      assertCacheValue(cache2, localKey, "value2");
      assertLoadsAndReset(cache1, 0, cache2, 0);

      MagicKey nonLocalKey = new MagicKey("nonLocal", cache2);
      cache1.put(nonLocalKey, "value");
      // Write skew check needs the previous version on the originator AND on the primary owner
      int cache1Loads = isTxCache() ? 1 : 0;
      assertLoadsAndReset(cache1, cache1Loads, cache2, 1);

      assertCacheValue(cache2, nonLocalKey, "value");
      assertLoadsAndReset(cache1, 0, cache2, 0);

      final AdvancedCache<MagicKey, String> cache1SkipRemoteAndStores =
            cache1LocalOnly.withFlags(SKIP_CACHE_LOAD);
      MagicKey localKey2 = new MagicKey("local2", cache1);
      cache1SkipRemoteAndStores.put(localKey2, "value");
      // CACHE_MODE_LOCAL operation is not replicated with the PrepareCommand and WSC is not executed,
      // but the entry is committed on the origin
      assertLoadsAndReset(cache1, 0, cache2, 0);

      assertCacheValue(cache1, localKey2, "value");
      // localKey2 isn't in memory, looks into store
      assertCacheValue(cache2, localKey2, null);
      assertLoadsAndReset(cache1, 0, cache2, 1);

      assertCacheValue(cache2, localKey2, null);
      assertLoadsAndReset(cache1, 0, cache2, 1);
      assertCacheValue(cache2.withFlags(SKIP_CACHE_LOAD), localKey2, null);
      assertLoadsAndReset(cache1, 0, cache2, 0);

      // Options on cache1SkipRemoteAndStores did NOT affect this cache
      MagicKey localKey3 = new MagicKey("local3", cache1);
      assertCacheValue(cache1LocalOnly, localKey3, null);
      assertLoadsAndReset(cache1, 1, cache2, 0);
   }

   public void testWithFlagsAndDelegateCache() {
      final AdvancedCache<Integer, String> c1 =
            new CustomDelegateCache<>(this.<Integer, String>advancedCache(0, cacheName));
      final AdvancedCache<Integer, String> c2 = advancedCache(1, cacheName);

      c1.withFlags(CACHE_MODE_LOCAL).put(1, "v1");
      assertCacheValue(c2, 1, null);
   }

   public void testReplicateSkipCacheLoad(Method m) {
      final AdvancedCache<Object, String> cache1 = advancedCache(0, cacheName);
      final AdvancedCache<Object, String> cache2 = advancedCache(1, cacheName);
      assertLoadsAndReset(cache1, 0, cache2, 0);

      final String v = v(m, 1);
      final Object k = getKeyForCache(0, cacheName);
      cache1.withFlags(Flag.SKIP_CACHE_LOAD).put(k, v);
      // The write-skew check tries to load it from persistence.
      assertLoadsAndReset(cache1, isTxCache() ? 1 : 0, cache2, 0);

      assertCacheValue(cache2, k, v);
      assertLoadsAndReset(cache1, 0, cache2, 0);
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
      assertLoadsAndReset(cache1, 0, cache2, 0);

      final String v = v(m, 1);
      final String k = k(m, 1);
      withTx(cache1.getTransactionManager(), (Callable<Void>) () -> {
         cache1.withFlags(Flag.SKIP_CACHE_LOAD).put(k, v);
         return null;
      });
      // The write-skew check tries to load it from persistence on the primary owner.
      assertLoadsAndReset(cache1, isPrimaryOwner(cache1, k) ? 1 : 0, cache2, isPrimaryOwner(cache2, k) ? 1 : 0);

      assertCacheValue(cache2, k, v);
      assertLoadsAndReset(cache1, 0, cache2, 0);
   }

   public static class CustomDelegateCache<K, V>
         extends AbstractDelegatingAdvancedCache<K, V> {

      public CustomDelegateCache(AdvancedCache<K, V> cache) {
         super(cache, CustomDelegateCache::new);
      }
   }

   private void assertLoadsAndReset(Cache<?, ?> cache1, int expected1, Cache<?, ?> cache2, int expected2) {
      DummyInMemoryStore store1 = getCacheStore(cache1);
      DummyInMemoryStore store2 = getCacheStore(cache2);
      assertEquals(expected1, (int) store1.stats().get("load"));
      assertEquals(expected2, (int) store2.stats().get("load"));
      store1.clearStats();
      store2.clearStats();
   }

   protected final void assertCacheValue(Cache<?, ?> cache, Object key, Object value) {
      assertEquals("Wrong value for key '" + key + "' in cache '" + cache + "'.", value, cache.get(key));
   }

   private boolean isPrimaryOwner(Cache<?, ?> cache, Object key) {
      return TestingUtil.extractComponent(cache, ClusteringDependentLogic.class).getCacheTopology().getDistribution(key).isPrimary();
   }

   private boolean isTxCache() {
      return advancedCache(0, cacheName).getCacheConfiguration().transaction().transactionMode().isTransactional();
   }
}
