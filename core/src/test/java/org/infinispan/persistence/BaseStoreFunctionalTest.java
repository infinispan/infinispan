package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


import static org.infinispan.test.TestingUtil.withCacheManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;


/**
 * This is a base functional test class containing tests that should be executed for each cache store/loader
 * implementation. As these are functional tests, they should interact against Cache/CacheManager only and any access to
 * the underlying cache store/loader should be done to verify contents.
 */
@Test(groups = "unit", testName = "persistence.BaseStoreFunctionalTest")
public abstract class BaseStoreFunctionalTest extends AbstractInfinispanTest {

   protected abstract PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload);

   protected Set<String> cacheNames = new HashSet<String>();

   protected Object wrap(String key, String value) {
      return value;
   }

   protected String unwrap(Object wrapped) {
      return (String) wrapped;
   }

   public void testTwoCachesSameCacheStore() {
      EmbeddedCacheManager localCacheManager = TestCacheManagerFactory.createCacheManager(false);
      try {
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.read(localCacheManager.getDefaultCacheConfiguration());
         createCacheStoreConfig(cb.persistence(), false);
         Configuration c = cb.build();
         localCacheManager.defineConfiguration("first", c);
         localCacheManager.defineConfiguration("second", c);
         cacheNames.add("first");
         cacheNames.add("second");

         Cache<String, Object> first = localCacheManager.getCache("first");
         Cache<String, Object> second = localCacheManager.getCache("second");

         first.start();
         second.start();

         first.put("key", wrap("key", "val"));
         assertEquals("val", unwrap(first.get("key")));
         assertNull(second.get("key"));

         second.put("key2", wrap("key2", "val2"));
         assertEquals("val2", unwrap(second.get("key2")));
         assertNull(first.get("key2"));
      } finally {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   public void testPreloadAndExpiry() {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), true);
      CacheContainer local = TestCacheManagerFactory.createCacheManager(cb);
      try {
         Cache<String, Object> cache = local.getCache();
         cacheNames.add(cache.getName());
         cache.start();

         assert cache.getCacheConfiguration().persistence().preload();

         cache.put("k1", wrap("k1", "v"));
         cache.put("k2", wrap("k2", "v"), 111111, TimeUnit.MILLISECONDS);
         cache.put("k3", wrap("k3", "v"), -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
         cache.put("k4", wrap("k4", "v"), 333333, TimeUnit.MILLISECONDS, 444444, TimeUnit.MILLISECONDS);

         assertCacheEntry(cache, "k1", "v", -1, -1);
         assertCacheEntry(cache, "k2", "v", 111111, -1);
         assertCacheEntry(cache, "k3", "v", -1, 222222);
         assertCacheEntry(cache, "k4", "v", 333333, 444444);
         cache.stop();

         cache.start();

         assertCacheEntry(cache, "k1", "v", -1, -1);
         assertCacheEntry(cache, "k2", "v", 111111, -1);
         assertCacheEntry(cache, "k3", "v", -1, 222222);
         assertCacheEntry(cache, "k4", "v", 333333, 444444);
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

   public void testPreloadStoredAsBinary() {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), true).storeAsBinary().enable();

      CacheContainer local = TestCacheManagerFactory.createCacheManager(cb);
      try {
         Cache<String, Pojo> cache = local.getCache();
         cacheNames.add(cache.getName());
         cache.start();

         assert cache.getCacheConfiguration().persistence().preload();
         assert cache.getCacheConfiguration().storeAsBinary().enabled();

         cache.put("k1", new Pojo());
         cache.put("k2", new Pojo(), 111111, TimeUnit.MILLISECONDS);
         cache.put("k3", new Pojo(), -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
         cache.put("k4", new Pojo(), 333333, TimeUnit.MILLISECONDS, 444444, TimeUnit.MILLISECONDS);

         cache.stop();

         cache.start();

         cache.entrySet();
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

   public static class Pojo implements Serializable {
   }

   public void testRestoreAtomicMap(Method m) {
      CacheContainer localCacheContainer = getContainerWithCacheLoader(null, false);
      try {
         Cache<String, Object> cache = localCacheContainer.getCache();
         cacheNames.add(cache.getName());
         AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, m.getName());
         map.put("a", "b");

         //evict from memory
         cache.evict(m.getName());

         // now re-retrieve the map
         assertEquals("b", AtomicMapLookup.getAtomicMap(cache, m.getName()).get("a"));
      } finally {
         TestingUtil.killCacheManagers(localCacheContainer);
      }
   }

   @Test(groups = "unstable")
   public void testRestoreTransactionalAtomicMap(Method m) throws Exception {
      CacheContainer localCacheContainer = getContainerWithCacheLoader(null, false);
      try {
         Cache<String, Object> cache = localCacheContainer.getCache();
         cacheNames.add(cache.getName());
         TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
         tm.begin();
         final AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, m.getName());
         map.put("a", "b");
         tm.commit();

         //evict from memory
         cache.evict(m.getName());

         // now re-retrieve the map and make sure we see the diffs
         assertEquals("b", AtomicMapLookup.getAtomicMap(cache, m.getName()).get("a"));
      } finally {
         TestingUtil.killCacheManagers(localCacheContainer);
      }
   }

   public void testStoreByteArrays(final Method m) throws PersistenceException {
      ConfigurationBuilder base = new ConfigurationBuilder();
      base.dataContainer().keyEquivalence(ByteArrayEquivalence.INSTANCE);
      // we need to purge the container when loading, because we could try to compare
      // some old entry using ByteArrayEquivalence and this throws ClassCastException
      // for non-byte[] arguments
      withCacheManager(new CacheManagerCallable(getContainerWithCacheLoader(base.build(), true)) {
         @Override
         public void call() {
            Cache<byte[], byte[]> cache = cm.getCache(m.getName());
            byte[] key = {1, 2, 3};
            byte[] value = {4, 5, 6};
            cache.put(key, value);
            // Lookup in memory, sanity check
            byte[] lookupKey = {1, 2, 3};
            byte[] found = cache.get(lookupKey);
            assertNotNull(found);
            assertArrayEquals(value, found);
            cache.evict(key);
            // Lookup in cache store
            found = cache.get(lookupKey);
            assertNotNull(found);
            assertArrayEquals(value, found);
         }
      });
   }

   private EmbeddedCacheManager getContainerWithCacheLoader(Configuration base, boolean purge) {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      if (base != null)
         cfg.read(base);

      cfg
         .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL);
      createCacheStoreConfig(cfg.persistence(), false);
      cfg.persistence().stores().get(0).purgeOnStartup(purge);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   private void assertCacheEntry(Cache cache, String key, String value, long lifespanMillis, long maxIdleMillis) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assertNotNull(ice);
      assertEquals(value, unwrap(ice.getValue()));
      assertEquals(lifespanMillis, ice.getLifespan());
      assertEquals(maxIdleMillis, ice.getMaxIdle());
      if (lifespanMillis > -1) assert ice.getCreated() > -1 : "Lifespan is set but created time is not";
      if (maxIdleMillis > -1) assert ice.getLastUsed() > -1 : "Max idle is set but last used is not";

   }
}
