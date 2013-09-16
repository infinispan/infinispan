package org.infinispan.api.flags;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.SKIP_CACHE_STORE;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.infinispan.test.TestingUtil.withTx;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.AbstractDelegatingAdvancedCache;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
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

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder
            .locking().writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ)
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .persistence().addStore(UnnecessaryLoadingTest.CountingStoreConfigurationBuilder.class)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .transaction().syncCommitPhase(true);
      createClusteredCaches(2, "replication", builder);
   }

   UnnecessaryLoadingTest.CountingStore getCacheStore(Cache cache) {
      return (UnnecessaryLoadingTest.CountingStore) TestingUtil.getFirstLoader(cache);
   }

   public void testWithFlagsSemantics() {
      AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;

      AdvancedCache<String, String> cache1LocalOnly =
            cache1.withFlags(CACHE_MODE_LOCAL);
      cache1LocalOnly.put("key", "value1");
      cache2.withFlags(CACHE_MODE_LOCAL).put("key", "value2");
      assert cache1.get("key").equals("value1");
      assert cache2.get("key").equals("value2");

      assert getCacheStore(cache1).numLoads == 1;
      assert getCacheStore(cache2).numLoads == 1;
      assert getCacheStore(cache2) != getCacheStore(cache1);

      cache1.put("nonLocal", "value");
      assert "value".equals(cache2.get("nonLocal"));
      assert getCacheStore(cache1).numLoads == 2;
      assert getCacheStore(cache2).numLoads == 1; //not incremented since ISPN-1642

      AdvancedCache<String, String> cache1SkipRemoteAndStores = cache1LocalOnly.withFlags(SKIP_CACHE_STORE);
      cache1SkipRemoteAndStores.put("again", "value");
      assert getCacheStore(cache1).numLoads == 2;
      assert getCacheStore(cache2).numLoads == 1;
      assert cache1.get("again").equals("value");
      assert cache2.get("again") == null;

      assert getCacheStore(cache1).numLoads == 2;
      assert getCacheStore(cache2).numLoads == 2; //"again" wasn't found in cache, looks into store

      assert cache2.get("again") == null;
      assert getCacheStore(cache2).numLoads == 3;
      assert cache2.withFlags(SKIP_CACHE_STORE).get("again") == null;
      assert getCacheStore(cache2).numLoads == 3;

      assert getCacheStore(cache1).numLoads == 2;
      assert cache1LocalOnly.get("localStored") == null;
      assert getCacheStore(cache1).numLoads == 3; //options on cache1SkipRemoteAndStores did NOT affect this cache
   }

   public void testWithFlagsAndDelegateCache() {
      AdvancedCache<Integer, String> c1 = advancedCache(0, "replication");
      AdvancedCache<Integer, String> c2 = advancedCache(1, "replication");

      c1 = new CustomDelegateCache<Integer, String>(c1);

      c1.withFlags(CACHE_MODE_LOCAL).put(1, "v1");
      assertEquals(null, c2.get(1));
   }

   public void testReplicateSkipCacheLoad(Method m) {
      AdvancedCache<String, String> cache1 = advancedCache(0,"replication");
      AdvancedCache<String, String> cache2 = advancedCache(1,"replication");
      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;

      final String v = v(m, 1);
      final String k = k(m, 1);
      cache1.withFlags(Flag.SKIP_CACHE_LOAD).put(k, v);
      assert v.equals(cache2.get(k));

      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;
   }

   public void testReplicateSkipCacheLoaderWithinTxInCoordinator(Method m) throws Exception {
      final AdvancedCache<String, String> cache1 = advancedCache(0, "replication");
      final AdvancedCache<String, String> cache2 = advancedCache(1, "replication");
      doReplicateSkipCacheLoaderWithinTx(m, cache1, cache2);
   }

   public void testReplicateSkipCacheLoaderWithinTxInNonCoordinator(Method m) throws Exception {
      final AdvancedCache<String, String> cache1 =
            this.<String, String>cache(0,"replication").getAdvancedCache();
      final AdvancedCache<String, String> cache2 =
            this.<String, String>cache(1, "replication").getAdvancedCache();
      doReplicateSkipCacheLoaderWithinTx(m, cache2, cache1);
   }

   private void doReplicateSkipCacheLoaderWithinTx(Method m,
         final AdvancedCache<String, String> cache1,
         AdvancedCache<String, String> cache2) throws Exception {
      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;

      final String v = v(m, 1);
      final String k = k(m, 1);
      withTx(cache1.getTransactionManager(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache1.withFlags(Flag.SKIP_CACHE_LOAD).put(k, v);
            return null;
         }
      });

      assert v.equals(cache2.get(k));
      assert getCacheStore(cache1).numLoads == 0;
      assert getCacheStore(cache2).numLoads == 0;
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

}
