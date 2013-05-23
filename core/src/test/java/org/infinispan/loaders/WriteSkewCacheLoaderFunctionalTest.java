package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.test.TestingUtil.withTx;
import static org.testng.AssertJUnit.*;

/**
 * Tests write skew functionality when interacting with a cache loader.
 *
 * @author Pedro Ruivo
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "loaders.WriteSkewCacheLoaderFunctionalTest")
public class WriteSkewCacheLoaderFunctionalTest extends SingleCacheManagerTest {

   CacheStore store;
   static final long LIFESPAN = 60000000; // very large lifespan so nothing actually expires

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = defineConfiguration();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(builder);
      store = TestingUtil.extractComponent(cm.getCache(), CacheLoaderManager.class).getCacheStore();
      return cm;
   }

   private ConfigurationBuilder defineConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .clustering().cacheMode(CacheMode.REPL_SYNC).sync()
            .loaders().preload(true).addLoader(DummyInMemoryCacheStoreConfigurationBuilder.class)
            .storeName(this.getClass().getName());
      return builder;
   }

   private void assertInCacheAndStore(Cache cache, CacheStore store, Object key, Object value) throws CacheLoaderException {
      assertInCacheAndStore(cache, store, key, value, -1);
   }

   private void assertInCacheAndStore(Cache cache, CacheStore store, Object key, Object value, long lifespanMillis) throws CacheLoaderException {
      InternalCacheEntry se = cache.getAdvancedCache().getDataContainer().get(key);
      assertStoredEntry(se, value, lifespanMillis, "Cache", key);
      se = store.load(key);
      assertStoredEntry(se, value, lifespanMillis, "Store", key);
   }

   private void assertStoredEntry(InternalCacheEntry entry, Object expectedValue, long expectedLifespan, String src, Object key) {
      assertNotNull(src + " entry for key " + key + " should NOT be null", entry);
      assertEquals(src + " should contain value " + expectedValue + " under key " + entry.getKey() + " but was " + entry.getValue() + ". Entry is " + entry,
            expectedValue, entry.getValue());
      assertEquals(src + " expected lifespan for key " + key + " to be " + expectedLifespan + " but was " + entry.getLifespan() + ". Entry is " + entry,
            expectedLifespan, entry.getLifespan());
   }

   private <T> void assertNotInCacheAndStore(Cache cache, CacheStore store, T... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assertFalse("Cache should not contain key " + key, cache.getAdvancedCache().getDataContainer().containsKey(key));
         assertFalse("Store should not contain key " + key, store.containsKey(key));
      }
   }

   public void testPreloadingInTransactionalCache() throws Exception {
      assertTrue(cache.getCacheConfiguration().loaders().preload());

      assertNotInCacheAndStore(cache, store, "k1", "k2", "k3", "k4");

      cache.put("k1", "v1");
      cache.put("k2", "v2", LIFESPAN, MILLISECONDS);
      cache.put("k3", "v3");
      cache.put("k4", "v4", LIFESPAN, MILLISECONDS);

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(cache, store, "k" + i, "v" + i);
         else
            assertInCacheAndStore(cache, store, "k" + i, "v" + i, LIFESPAN);
      }

      DataContainer c = cache.getAdvancedCache().getDataContainer();

      assertEquals(4, c.size());
      cache.stop();
      assertEquals(0, c.size());

      cache.start();
      assertTrue(cache.getCacheConfiguration().loaders().preload());

      c = cache.getAdvancedCache().getDataContainer();
      assertEquals(4, c.size());

      // Re-retrieve since the old reference might not be usable
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(cache, store, "k" + i, "v" + i);
         else
            assertInCacheAndStore(cache, store, "k" + i, "v" + i, LIFESPAN);
      }

      withTx(cache.getAdvancedCache().getTransactionManager(), new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            assertEquals("v1", cache.get("k1"));
            cache.put("k1", "new-v1");
            return null;
         }
      });
   }

}