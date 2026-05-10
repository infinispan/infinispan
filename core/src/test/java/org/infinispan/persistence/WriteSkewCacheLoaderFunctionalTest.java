package org.infinispan.persistence;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.test.TestingUtil.withTx;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Tests write skew functionality when interacting with a cache loader.
 *
 * @author Pedro Ruivo
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "persistence.WriteSkewCacheLoaderFunctionalTest")
public class WriteSkewCacheLoaderFunctionalTest extends SingleCacheManagerTest {

   DummyInMemoryStore loader;
   static final long LIFESPAN = 60000000; // very large lifespan so nothing actually expires

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = defineConfiguration();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(builder);
      loader = TestingUtil.getFirstStore(cm.getCache());
      return cm;
   }

   private ConfigurationBuilder defineConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .clustering().cacheMode(CacheMode.REPL_SYNC)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(this.getClass().getName()).preload(true);
      return builder;
   }

   private void assertInCacheAndStore(Cache cache, DummyInMemoryStore loader, Object key, Object value) throws PersistenceException {
      assertInCacheAndStore(cache, loader, key, value, -1);
   }

   private void assertInCacheAndStore(Cache cache, DummyInMemoryStore store, Object key, Object value, long lifespanMillis) throws PersistenceException {
      InternalCacheValue icv = cache.getAdvancedCache().getDataContainer().peek(key).toInternalCacheValue();
      assertStoredEntry(icv.getValue(), value, icv.getLifespan(), lifespanMillis, "Cache", key);
      assertNotNull(icv.getInternalMetadata().entryVersion(), "For :" + icv);
      MarshallableEntry<?, ?> load = store.loadEntry(key);
      assertStoredEntry(load.getValue(), value, load.getMetadata() == null ? -1 : load.getMetadata().lifespan(), lifespanMillis, "Store", key);
      assertNotNull(load.getInternalMetadata().entryVersion(), "For :" + load);
   }

   private void assertStoredEntry(Object value, Object expectedValue, long lifespanMillis, long expectedLifespan, String src, Object key) {
      assertNotNull(value, src + " entry for key " + key + " should NOT be null");
      assertEquals(expectedValue, value, src + " should contain value " + expectedValue + " under key " + key + " but was " + value);
      assertEquals(expectedLifespan, lifespanMillis, src + " expected lifespan for key " + key + " to be " + expectedLifespan + " but was " + lifespanMillis);
   }

   private <T> void assertNotInCacheAndStore(Cache cache, DummyInMemoryStore store, Collection<T> keys) throws PersistenceException {
      for (Object key : keys) {
         assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), "Cache should not contain key " + key);
         assertFalse(store.contains(key), "Store should not contain key " + key);
      }
   }

   public void testPreloadingInTransactionalCache() throws Exception {
      assertTrue(cache.getCacheConfiguration().persistence().preload());

      assertNotInCacheAndStore(cache, loader, Arrays.asList("k1", "k2", "k3", "k4"));

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

      DataContainer<?, ?> c = cache.getAdvancedCache().getDataContainer();

      assertEquals(4, c.size());
      cache.stop();
      assertEquals(0, c.size());

      cache.start();
      assertTrue(cache.getCacheConfiguration().persistence().preload());

      c = cache.getAdvancedCache().getDataContainer();
      assertEquals(4, c.size());

      // Re-retrieve since the old reference might not be usable
      loader = TestingUtil.getFirstStore(cache);
      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(cache, loader, "k" + i, "v" + i);
         else
            assertInCacheAndStore(cache, loader, "k" + i, "v" + i, LIFESPAN);
      }

      withTx(cache.getAdvancedCache().getTransactionManager(), (Callable<Void>) () -> {
         assertEquals("v1", cache.get("k1"));
         cache.put("k1", "new-v1");
         return null;
      });
   }

}
