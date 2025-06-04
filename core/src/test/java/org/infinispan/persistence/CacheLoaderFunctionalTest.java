package org.infinispan.persistence;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.api.mvcc.LockAssert.assertNoLocks;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/**
 * Tests the interceptor chain and surrounding logic
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "persistence.CacheLoaderFunctionalTest")
public class CacheLoaderFunctionalTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(CacheLoaderFunctionalTest.class);

   private boolean segmented;

   Cache<String, String> cache;
   DummyInMemoryStore store;
   DummyInMemoryStore writer;
   TransactionManager tm;
   ConfigurationBuilder cfg;
   EmbeddedCacheManager cm;

   long lifespan = 60000000; // very large lifespan so nothing actually expires

   @BeforeMethod(alwaysRun = true)
   public void setUp() {
      cfg = getConfiguration();
      configure(cfg);
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = getCache(cm);
      store = TestingUtil.getFirstStore(cache);
      writer = TestingUtil.getFirstStore(cache);
      tm = TestingUtil.getTransactionManager(cache);
   }

   public CacheLoaderFunctionalTest segmented(boolean segmented) {
      this.segmented = segmented;
      return this;
   }

   @Override
   protected String parameters() {
      return "[" + segmented + "]";
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new CacheLoaderFunctionalTest().segmented(true),
            new CacheLoaderFunctionalTest().segmented(false),
      };
   }

   protected ConfigurationBuilder getConfiguration() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence()
         .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .segmented(segmented)
         .storeName(this.getClass().getName()) // in order to use the same store
         .transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      return cfg;
   }

   protected Cache<String, String> getCache(EmbeddedCacheManager cm) {
      return cm.getCache();
   }

   protected Cache<String, String> getCache(EmbeddedCacheManager cm, String name) {
      return cm.getCache(name);
   }

   protected void configure(ConfigurationBuilder cb) { }

   @AfterMethod(alwaysRun = true)
   public void tearDown() throws PersistenceException {
      if (writer != null) {
         writer.clear();
      }
      TestingUtil.killCacheManagers(cm);
      cache = null;
      cm = null;
      cfg = null;
      tm = null;
      store = null;
   }

   private void assertInCacheAndStore(String key, Object value) throws PersistenceException {
      assertInCacheAndStore(key, value, -1);
   }

   private void assertInCacheAndStore(String key, Object value, long lifespanMillis) throws PersistenceException {
      assertInCacheAndStore(cache, store, key, value, lifespanMillis);
   }


   private <K> void assertInCacheAndStore(Cache<? super K, ?> cache, DummyInMemoryStore store, K key, Object value) throws PersistenceException {
      assertInCacheAndStore(cache, store, key, value, -1);
   }

   private <K> void assertInCacheAndStore(Cache<? super K, ?> cache, DummyInMemoryStore loader, K key, Object value, long lifespanMillis) throws PersistenceException {
      InternalCacheEntry se = cache.getAdvancedCache().getDataContainer().peek(key);
      testStoredEntry(se.getValue(), value, se.getLifespan(), lifespanMillis, "Cache", key);
      MarshallableEntry load = loader.loadEntry(key);
      testStoredEntry(load.getValue(), value, load.getMetadata() == null ? -1 : load.getMetadata().lifespan(), lifespanMillis, "Store", key);
   }

   private void testStoredEntry(Object value, Object expectedValue, long lifespan, long expectedLifespan, String src, Object key) {
      assertEquals("Wrong value on " + src, expectedValue, value);
      assertEquals("Wrong lifespan on " + src, expectedLifespan, lifespan);
   }

   private static <K> void assertNotInCacheAndStore(Cache<? super K, ?> cache, DummyInMemoryStore store, K... keys) throws PersistenceException {
      for (K key : keys) {
         assertFalse("Cache should not contain key " + key, cache.getAdvancedCache().getDataContainer().containsKey(key));
         assertFalse("Store should not contain key " + key, store.contains(key));
      }
   }

   private void assertNotInCacheAndStore(String... keys) throws PersistenceException {
      assertNotInCacheAndStore(cache, store, keys);
   }

   private void assertInStoreNotInCache(String... keys) throws PersistenceException {
      assertInStoreNotInCache(cache, store, keys);
   }

   private static <K> void assertInStoreNotInCache(Cache<? super K, ?> cache, DummyInMemoryStore store, K... keys) throws PersistenceException {
      for (K key : keys) {
         assertFalse("Cache should not contain key " + key, cache.getAdvancedCache().getDataContainer().containsKey(key));
         assertTrue("Store should contain key " + key, store.contains(key));
      }
   }

   private void assertInCacheAndNotInStore(String... keys) throws PersistenceException {
      assertInCacheAndNotInStore(cache, store, keys);
   }

   private static <K> void assertInCacheAndNotInStore(Cache<? super K, ?> cache, DummyInMemoryStore store, K... keys) throws PersistenceException {
      for (K key : keys) {
         assert cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assertFalse("Store should contain key " + key, store.contains(key));
      }
   }


   public void testStoreAndRetrieve() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2", "k3", "k4", "k5", "k6", "k7");

      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);
      cache.putAll(Collections.singletonMap("k3", "v3"));
      cache.putAll(Collections.singletonMap("k4", "v4"), lifespan, MILLISECONDS);
      cache.putIfAbsent("k5", "v5");
      cache.putIfAbsent("k6", "v6", lifespan, MILLISECONDS);
      cache.putIfAbsent("k5", "v5-SHOULD-NOT-PUT");
      cache.putIfAbsent("k6", "v6-SHOULD-NOT-PUT", lifespan, MILLISECONDS);
      cache.putForExternalRead("k7", "v7");
      cache.putForExternalRead("k7", "v7-SHOULD-NOT-PUT");

      for (int i = 1; i < 8; i++) {
         // even numbers have lifespans
         if (i % 2 == 1)
            assertInCacheAndStore("k" + i, "v" + i);
         else
            assertInCacheAndStore("k" + i, "v" + i, lifespan);
      }

      assert !cache.remove("k1", "some rubbish");

      for (int i = 1; i < 8; i++) {
         // even numbers have lifespans
         if (i % 2 == 1)
            assertInCacheAndStore("k" + i, "v" + i);
         else
            assertInCacheAndStore("k" + i, "v" + i, lifespan);
      }

      log.debugf("cache.get(\"k1\") = %s", cache.get("k1"));
      boolean removed = cache.remove("k1", "v1");
      assertTrue(removed);
      log.debugf("cache.get(\"k1\") = %s", cache.get("k1"));
      assertEquals("v2", cache.remove("k2"));

      assertNotInCacheAndStore("k1", "k2");

      for (int i = 3; i < 8; i++) {
         // even numbers have lifespans
         if (i % 2 == 1)
            assertInCacheAndStore("k" + i, "v" + i);
         else
            assertInCacheAndStore("k" + i, "v" + i, lifespan);
      }

      cache.clear();
      assertNotInCacheAndStore("k1", "k2", "k3", "k4", "k5", "k6", "k7");
   }

   public void testReplaceMethods() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2", "k3", "k4");

      cache.replace("k1", "v1-SHOULD-NOT-STORE");
      assertNoLocks(cache);
      cache.replace("k2", "v2-SHOULD-NOT-STORE", lifespan, MILLISECONDS);
      assertNoLocks(cache);

      assertNotInCacheAndStore("k1", "k2", "k3", "k4");

      cache.put("k1", "v1");
      assertNoLocks(cache);
      cache.put("k2", "v2");
      assertNoLocks(cache);
      cache.put("k3", "v3");
      assertNoLocks(cache);
      cache.put("k4", "v4");
      assertNoLocks(cache);

      for (int i = 1; i < 5; i++) assertInCacheAndStore("k" + i, "v" + i);

      cache.replace("k1", "v1-SHOULD-NOT-STORE", "v1-STILL-SHOULD-NOT-STORE");
      assertNoLocks(cache);
      cache.replace("k2", "v2-SHOULD-NOT-STORE", "v2-STILL-SHOULD-NOT-STORE", lifespan, MILLISECONDS);
      assertNoLocks(cache);

      for (int i = 1; i < 5; i++) assertInCacheAndStore("k" + i, "v" + i);

      cache.replace("k1", "v1-REPLACED");
      assertNoLocks(cache);
      cache.replace("k2", "v2-REPLACED", lifespan, MILLISECONDS);
      assertInCacheAndStore("k2", "v2-REPLACED", lifespan);
      assertNoLocks(cache);
      cache.replace("k3", "v3", "v3-REPLACED");
      assertNoLocks(cache);
      cache.replace("k4", "v4", "v4-REPLACED", lifespan, MILLISECONDS);
      assertNoLocks(cache);

      for (int i = 1; i < 5; i++) {
         // even numbers have lifespans
         if (i % 2 == 1)
            assertInCacheAndStore("k" + i, "v" + i + "-REPLACED");
         else
            assertInCacheAndStore("k" + i, "v" + i + "-REPLACED", lifespan);
      }

      assertNoLocks(cache);
   }

   public void testLoading() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2", "k3", "k4");
      if (Configurations.isTxVersioned(cache.getCacheConfiguration())) {
         for (int i = 1; i < 5; i++) writer.write(MarshalledEntryUtil.createWithVersion("k" + i, "v" + i, cache));
      } else {
         for (int i = 1; i < 5; i++) writer.write(MarshalledEntryUtil.create("k" + i, "v" + i, cache));
      }
      for (int i = 1; i < 5; i++) assertEquals("v" + i, cache.get("k" + i));
      // make sure we have no stale locks!!
      assertNoLocks(cache);

      for (int i = 1; i < 5; i++) cache.evict("k" + i);
      // make sure we have no stale locks!!
      assertNoLocks(cache);

      assertEquals("v1", cache.putIfAbsent("k1", "v1-SHOULD-NOT-STORE"));
      assertEquals("v2", cache.remove("k2"));
      assertEquals("v3", cache.replace("k3", "v3-REPLACED"));
      assertTrue(cache.replace("k4", "v4", "v4-REPLACED"));
      // make sure we have no stale locks!!
      assertNoLocks(cache);

      int size = cache.size();
      assertEquals("Expected the cache to contain 3 elements but contained " + cache.entrySet(), 3, size);

      for (int i = 1; i < 5; i++) cache.evict("k" + i);
      // make sure we have no stale locks!!
      assertNoLocks(cache);

      assertEquals(0, cache.getAdvancedCache().getDataContainer().size()); // cache size ops will not trigger a load

      cache.clear(); // this should propagate to the loader though
      assertNotInCacheAndStore("k1", "k2", "k3", "k4");
      // make sure we have no stale locks!!
      assertNoLocks(cache);
   }

   public void testPreloading() throws Exception {
      ConfigurationBuilder preloadingCfg = newPreloadConfiguration(cfg.build(), this.getClass().getName() + "preloadingCache");
      doPreloadingTest(preloadingCfg.build(), "preloadingCache");
   }

   public void testPreloadingWithoutAutoCommit() throws Exception {
      ConfigurationBuilder preloadingCfg = newPreloadConfiguration(cfg.build(), this.getClass().getName() + "preloadingCache_2");
      preloadingCfg.transaction().autoCommit(false);
      doPreloadingTest(preloadingCfg.build(), "preloadingCache_2");
   }

   public void testPreloadingWithEvictionAndOneMaxEntry() throws Exception {
      ConfigurationBuilder preloadingCfg = newPreloadConfiguration(cfg.build(), this.getClass().getName() + "preloadingCache_3");
      preloadingCfg.memory().maxCount(1);
      doPreloadingTestWithEviction(preloadingCfg.build(), "preloadingCache_3");
   }

   public void testPreloadingWithEviction() throws Exception {
      ConfigurationBuilder preloadingCfg = newPreloadConfiguration(cfg.build(), this.getClass().getName() + "preloadingCache_4");
      preloadingCfg.memory().maxCount(3);
      doPreloadingTestWithEviction(preloadingCfg.build(), "preloadingCache_4");
   }

   ConfigurationBuilder newPreloadConfiguration(Configuration configuration, String storeName) {
      ConfigurationBuilder preloadingCfg = new ConfigurationBuilder();
      preloadingCfg.read(configuration, Combine.DEFAULT);
      preloadingCfg.persistence()
            .clearStores()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .segmented(segmented)
               .preload(true)
               .storeName(storeName);
      return preloadingCfg;
   }

   @Test(groups = "unstable")
   public void testPurgeOnStartup() throws PersistenceException {
      ConfigurationBuilder purgingCfg = new ConfigurationBuilder();
      purgingCfg.read(cfg.build(), Combine.DEFAULT);
      purgingCfg.persistence().clearStores().addStore(DummyInMemoryStoreConfigurationBuilder.class)
         .storeName("purgingCache").purgeOnStartup(true);
      cm.defineConfiguration("purgingCache", purgingCfg.build());
      Cache<String, String> purgingCache = getCache(cm, "purgingCache");
      DummyInMemoryStore purgingLoader = TestingUtil.getFirstStore(purgingCache);

      assertNotInCacheAndStore(purgingCache, purgingLoader, "k1", "k2", "k3", "k4");

      purgingCache.put("k1", "v1");
      purgingCache.put("k2", "v2", lifespan, MILLISECONDS);
      purgingCache.put("k3", "v3");
      purgingCache.put("k4", "v4", lifespan, MILLISECONDS);

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(purgingCache, purgingLoader, "k" + i, "v" + i);
         else
            assertInCacheAndStore(purgingCache, purgingLoader, "k" + i, "v" + i, lifespan);
      }

      DataContainer c = purgingCache.getAdvancedCache().getDataContainer();
      assertEquals(4, c.size());
      purgingCache.stop();
      assertEquals(0, c.size());

      purgingCache.start();
      purgingLoader = TestingUtil.getFirstStore(purgingCache);
      c = purgingCache.getAdvancedCache().getDataContainer();
      assertEquals(0, c.size());

      assertNotInCacheAndStore(purgingCache, purgingLoader, "k1", "k2", "k3", "k4");
   }

   public void testTransactionalWrites() throws Exception {
      assertEquals(ComponentStatus.RUNNING, cache.getStatus());
      assertNotInCacheAndStore("k1", "k2");

      tm.begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);
      Transaction t = tm.suspend();

      assertNotInCacheAndStore("k1", "k2");

      tm.resume(t);
      tm.commit();

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2", lifespan);

      tm.begin();
      cache.remove("k1");
      cache.remove("k2");
      t = tm.suspend();

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2", lifespan);
      tm.resume(t);
      tm.commit();

      assertNotInCacheAndStore("k1", "k2");

      tm.begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);
      t = tm.suspend();

      assertNotInCacheAndStore("k1", "k2");

      tm.resume(t);
      tm.rollback();

      assertNotInCacheAndStore("k1", "k2");
      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2", lifespan);

      tm.begin();
      cache.remove("k1");
      cache.remove("k2");
      t = tm.suspend();

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2", lifespan);
      tm.resume(t);
      tm.rollback();

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2", lifespan);
   }

   public void testTransactionalReplace(Method m) throws Exception {
      assertEquals(ComponentStatus.RUNNING, cache.getStatus());
      assertNotInCacheAndStore(k(m, 1));
      assertNotInCacheAndStore(k(m, 2));

      cache.put(k(m, 2), v(m));

      tm.begin();
      cache.put(k(m, 1), v(m, 1));
      cache.replace(k(m, 2), v(m, 1));
      Transaction t = tm.suspend();

      assertNotInCacheAndStore(k(m, 1));
      assertInCacheAndStore(k(m, 2), v(m));

      tm.resume(t);
      tm.commit();

      assertInCacheAndStore(k(m, 1), v(m, 1));
      assertInCacheAndStore(k(m, 2), v(m, 1));
   }

   public void testEvictAndRemove() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2");
      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);

      cache.evict("k1");
      cache.evict("k2");

      assertEquals("v1", cache.remove("k1"));
      assertEquals("v2", cache.remove("k2"));
   }

   public void testLoadingToMemory() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2");
      store.write(MarshalledEntryUtil.create("k1", "v1", cache));
      store.write(MarshalledEntryUtil.create("k2", "v2", cache));

      assertInStoreNotInCache("k1", "k2");

      assertEquals("v1", cache.get("k1"));
      assertEquals("v2", cache.get("k2"));

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2");

      store.delete("k1");
      store.delete("k2");

      assertInCacheAndNotInStore("k1", "k2");
      assertEquals("v1", cache.get("k1"));
      assertEquals("v2", cache.get("k2"));
   }

   public void testSkipLocking(Method m) {
      String name = m.getName();
      AdvancedCache<String, String> advancedCache = cache.getAdvancedCache();
      advancedCache.put("k-" + name, "v-" + name);
      advancedCache.withFlags(Flag.SKIP_LOCKING).put("k-" + name, "v2-" + name);
   }

   public void testDuplicatePersistence(Method m) throws Exception {
      String key = "k-" + m.getName();
      String value = "v-" + m.getName();
      cache.put(key, value);
      assertEquals(value, cache.get(key));

      cache.stop();
      cache.start();
      // A new writer is created after restart
      writer = TestingUtil.getFirstStore(cache);

      tm.begin();
      cache.containsKey(key); // Necessary call to force locks being acquired in advance
      cache.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key);
      cache.put(key, value);
      tm.commit();
      assertEquals(value, cache.get(key));
   }

   public void testNullFoundButLoaderReceivedValueLaterInTransaction() throws SystemException, NotSupportedException {
      assertNotInCacheAndStore("k1");
      tm.begin();
      try {
         assertNull(cache.get("k1"));

         // Now simulate that someone else wrote to the store while during our tx
         store.write(MarshalledEntryUtil.create("k1", "v1", cache));
         IsolationLevel level = cache.getCacheConfiguration().locking().lockIsolationLevel();
         switch(level) {
            case READ_COMMITTED:
               assertEquals("v1", cache.get("k1"));
               break;
            case REPEATABLE_READ:
               assertNull(cache.get("k1"));
               break;
            default:
               fail("Unsupported isolation " + level + " - please change test to add desired outcome for isolation level");
         }
      } finally {
         tm.rollback();
      }
   }

   public void testValuesForCacheLoader() {
      cache.putIfAbsent("k1", "v1");
      Set<String> copy1 = copyValues(cache);
      assertEquals(TestingUtil.setOf("v1"), copy1);

      cache.putIfAbsent("k2", "v2");
      Set<String> copy2 = copyValues(cache);
      assertEquals(TestingUtil.setOf("v1", "v2"), copy2);
   }

   private Set<String> copyValues(Cache<?, String> cache) {
      return new HashSet<>(cache.values());
   }


   protected void doPreloadingTest(Configuration preloadingCfg, String cacheName) throws Exception {
      assertTrue("Preload not enabled for preload test", preloadingCfg.persistence().preload());
      cm.defineConfiguration(cacheName, preloadingCfg);
      Cache<String, String> preloadingCache = getCache(cm, cacheName);
      DummyInMemoryStore preloadingCacheLoader = TestingUtil.getFirstStore(preloadingCache);

      assert preloadingCache.getCacheConfiguration().persistence().preload();

      assertNotInCacheAndStore(preloadingCache, preloadingCacheLoader, "k1", "k2", "k3", "k4");

      preloadingCache.getAdvancedCache().getTransactionManager().begin();
      preloadingCache.put("k1", "v1");
      preloadingCache.put("k2", "v2", lifespan, MILLISECONDS);
      preloadingCache.put("k3", "v3");
      preloadingCache.put("k4", "v4", lifespan, MILLISECONDS);
      preloadingCache.getAdvancedCache().getTransactionManager().commit();

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(preloadingCache, preloadingCacheLoader, "k" + i, "v" + i);
         else
            assertInCacheAndStore(preloadingCache, preloadingCacheLoader, "k" + i, "v" + i, lifespan);
      }

      DataContainer c = preloadingCache.getAdvancedCache().getDataContainer();
      assertEquals(4, c.size());
      preloadingCache.stop();
      assertEquals(0, c.size());

      preloadingCache.start();
      // The old store's marshaller is not working any more
      preloadingCacheLoader = TestingUtil.getFirstStore(preloadingCache);
      assert preloadingCache.getCacheConfiguration().persistence().preload();
      c = preloadingCache.getAdvancedCache().getDataContainer();
      assertEquals(4, c.size());

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(preloadingCache, preloadingCacheLoader, "k" + i, "v" + i);
         else
            assertInCacheAndStore(preloadingCache, preloadingCacheLoader, "k" + i, "v" + i, lifespan);
      }
   }

   protected void doPreloadingTestWithEviction(Configuration preloadingCfg, String cacheName) throws Exception {
      assertTrue("Preload not enabled for preload with eviction test", preloadingCfg.persistence().preload());
      assertTrue("Eviction not enabled for preload with eviction test", preloadingCfg.memory().isEvictionEnabled());

      cm.defineConfiguration(cacheName, preloadingCfg);

      final Cache<String, String> preloadingCache = getCache(cm, cacheName);
      final long expectedEntriesInContainer = Math.min(4L, preloadingCfg.memory().maxCount());
      DummyInMemoryStore preloadingCacheLoader = TestingUtil.getFirstStore(preloadingCache);

      assertTrue("Preload not enabled in cache configuration",
                 preloadingCache.getCacheConfiguration().persistence().preload());
      assertNotInCacheAndStore(preloadingCache, preloadingCacheLoader, "k1", "k2", "k3", "k4");

      preloadingCache.getAdvancedCache().getTransactionManager().begin();
      preloadingCache.put("k1", "v1");
      preloadingCache.put("k2", "v2", lifespan, MILLISECONDS);
      preloadingCache.put("k3", "v3");
      preloadingCache.put("k4", "v4", lifespan, MILLISECONDS);
      preloadingCache.getAdvancedCache().getTransactionManager().commit();

      DataContainer c = preloadingCache.getAdvancedCache().getDataContainer();
      assertEquals("Wrong number of entries in data container", expectedEntriesInContainer, c.size());

      for (int i = 1; i < 5; i++) {
         final String key = "k" + i;
         final Object value = "v" + i;
         final long lifespan = i % 2 == 1 ? -1 : this.lifespan;
         boolean found = false;
         InternalCacheEntry se = preloadingCache.getAdvancedCache().getDataContainer().peek(key);
         MarshallableEntry load = preloadingCacheLoader.loadEntry(key);
         if (se != null) {
            testStoredEntry(se.getValue(), value, se.getLifespan(), lifespan, "Cache", key);
            found = true;
         }
         if (load != null) {
            testStoredEntry(load.getValue(), value, load.getMetadata() == null ? -1 : load.getMetadata().lifespan(), lifespan, "Store", key);
            found = true;
         }
         assertTrue("Key not found.", found);
      }

      preloadingCache.stop();
      assertEquals("DataContainer still has entries after stop", 0, c.size());

      preloadingCache.start();
      // The old store's marshaller is not working any more
      preloadingCacheLoader = TestingUtil.getFirstStore(preloadingCache);

      assertTrue("Preload not enabled in cache configuration",
                 preloadingCache.getCacheConfiguration().persistence().preload());

      c = preloadingCache.getAdvancedCache().getDataContainer();
      assertEquals("Wrong number of entries in data container", expectedEntriesInContainer, c.size());

      for (int i = 1; i < 5; i++) {
         final String key = "k" + i;
         final Object value = "v" + i;
         final long lifespan = i % 2 == 1 ? -1 : this.lifespan;
         boolean found = false;
         InternalCacheEntry se = preloadingCache.getAdvancedCache().getDataContainer().peek(key);
         MarshallableEntry load = preloadingCacheLoader.loadEntry(key);
         if (se != null) {
            testStoredEntry(se.getValue(), value, se.getLifespan(), lifespan, "Cache", key);
            found = true;
         }
         if (load != null) {
            testStoredEntry(load.getValue(), value, load.getMetadata() == null ? -1 : load.getMetadata().lifespan(), lifespan, "Store", key);
            found = true;
         }
         assertTrue("Key not found.", found);
      }
   }

}
