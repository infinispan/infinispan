package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
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
@Test(groups = "functional", testName = "persistence.WriteSkewCacheLoaderFunctionalTest")
public class WriteSkewCacheLoaderFunctionalTest extends SingleCacheManagerTest {

   CacheLoader loader;
   static final long LIFESPAN = 60000000; // very large lifespan so nothing actually expires

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = defineConfiguration();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(builder);
      loader = TestingUtil.getFirstLoader(cm.getCache());
      return cm;
   }

   private ConfigurationBuilder defineConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .clustering().cacheMode(CacheMode.REPL_SYNC).sync()
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(this.getClass().getName()).preload(true);
      return builder;
   }

   private void assertInCacheAndStore(Cache cache, CacheLoader loader, Object key, Object value) throws CacheLoaderException {
      assertInCacheAndStore(cache, loader, key, value, -1);
   }

   private void assertInCacheAndStore(Cache cache, CacheLoader store, Object key, Object value, long lifespanMillis) throws CacheLoaderException {
      InternalCacheValue icv = cache.getAdvancedCache().getDataContainer().get(key).toInternalCacheValue();
      assertStoredEntry(icv.getValue(), value, icv.getLifespan(), lifespanMillis, "Cache", key);
      assertNotNull("For :" + icv, icv.getMetadata().version());
      MarshalledEntry load = store.load(key);
      assertStoredEntry(load.getValue(), value, load.getMetadata().lifespan(), lifespanMillis, "Store", key);
      assertNotNull("For :" + load, load.getMetadata().version());
   }

   private void assertStoredEntry(Object value, Object expectedValue, long lifespanMillis, long expectedLifespan, String src, Object key) {
      assertNotNull(src + " entry for key " + key + " should NOT be null", value);
      assertEquals(src + " should contain value " + expectedValue + " under key " + key + " but was " + value, expectedValue, value);
      assertEquals(src + " expected lifespan for key " + key + " to be " + expectedLifespan + " but was " + lifespanMillis, expectedLifespan, lifespanMillis);
   }

   private <T> void assertNotInCacheAndStore(Cache cache, CacheLoader store, T... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assertFalse("Cache should not contain key " + key, cache.getAdvancedCache().getDataContainer().containsKey(key));
         assertFalse("Store should not contain key " + key, store.contains(key));
      }
   }

   public void testPreloadingInTransactionalCache() throws Exception {
      assertTrue(cache.getCacheConfiguration().persistence().preload());

      assertNotInCacheAndStore(cache, loader, "k1", "k2", "k3", "k4");

      cache.put("k1", "v1");
      cache.put("k2", "v2", LIFESPAN, MILLISECONDS);
      cache.put("k3", "v3");
      cache.put("k4", "v4", LIFESPAN, MILLISECONDS);

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(cache, loader, "k" + i, "v" + i);
         else
            assertInCacheAndStore(cache, loader, "k" + i, "v" + i, LIFESPAN);
      }

      DataContainer c = cache.getAdvancedCache().getDataContainer();

      assertEquals(4, c.size());
      cache.stop();
      assertEquals(0, c.size());

      cache.start();
      assertTrue(cache.getCacheConfiguration().persistence().preload());

      c = cache.getAdvancedCache().getDataContainer();
      assertEquals(4, c.size());

      // Re-retrieve since the old reference might not be usable
      loader = TestingUtil.getFirstLoader(cache);
      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(cache, loader, "k" + i, "v" + i);
         else
            assertInCacheAndStore(cache, loader, "k" + i, "v" + i, LIFESPAN);
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