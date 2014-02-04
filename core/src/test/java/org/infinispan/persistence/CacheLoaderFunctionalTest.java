package org.infinispan.persistence;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.context.Flag;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import java.lang.reflect.Method;
import java.util.Collections;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.api.mvcc.LockAssert.assertNoLocks;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests the interceptor chain and surrounding logic
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "persistence.CacheLoaderFunctionalTest")
public class CacheLoaderFunctionalTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(CacheLoaderFunctionalTest.class);

   Cache<String, String> cache;
   AdvancedLoadWriteStore store;
   AdvancedCacheWriter writer;
   TransactionManager tm;
   ConfigurationBuilder cfg;
   EmbeddedCacheManager cm;
   StreamingMarshaller sm;

   long lifespan = 60000000; // very large lifespan so nothing actually expires

   @BeforeMethod(alwaysRun = true)
   public void setUp() {
      cfg = new ConfigurationBuilder();
      cfg.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .storeName(this.getClass().getName()) // in order to use the same store
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      configure(cfg);
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = (AdvancedLoadWriteStore) TestingUtil.getFirstLoader(cache);
      writer = (AdvancedCacheWriter) TestingUtil.getFirstLoader(cache);
      tm = TestingUtil.getTransactionManager(cache);
      sm = cache.getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   protected void configure(ConfigurationBuilder cb) { }

   @AfterMethod(alwaysRun = true)
   public void tearDown() throws PersistenceException {
      writer.clear();
      TestingUtil.killCacheManagers(cm);
      cache = null;
      cm = null;
      cfg = null;
      tm = null;
      store = null;
   }

   private void assertInCacheAndStore(Object key, Object value) throws PersistenceException {
      assertInCacheAndStore(key, value, -1);
   }

   private void assertInCacheAndStore(Object key, Object value, long lifespanMillis) throws PersistenceException {
      assertInCacheAndStore(cache, store, key, value, lifespanMillis);
   }


   private void assertInCacheAndStore(Cache<?, ?> cache, CacheLoader store, Object key, Object value) throws PersistenceException {
      assertInCacheAndStore(cache, store, key, value, -1);
   }

   private void assertInCacheAndStore(Cache<?, ?> cache, CacheLoader loader, Object key, Object value, long lifespanMillis) throws PersistenceException {
      InternalCacheEntry se = cache.getAdvancedCache().getDataContainer().get(key);
      testStoredEntry(se.getValue(), value, se.getLifespan(), lifespanMillis, "Cache", key);
      MarshalledEntry load = loader.load(key);
      testStoredEntry(load.getValue(), value, load.getMetadata() == null ? -1 : load.getMetadata().lifespan(), lifespanMillis, "Store", key);
   }

   private void testStoredEntry(Object value, Object expectedValue, long lifespan, long expectedLifespan, String src, Object key) {
      assert value != null : src + " icv for key " + key + " should NOT be null";
      assert value.equals(expectedValue) : src + " should contain value " + expectedValue + " under key " + key + " but was " + value;
      assert lifespan == expectedLifespan : src + " expected lifespan for key " + key + " to be " + expectedLifespan + " but was " + value;
   }

   private void assertNotInCacheAndStore(Cache<?, ?> cache, CacheLoader store, Object... keys) throws PersistenceException {
      for (Object key : keys) {
         assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert !store.contains(key) : "Store should not contain key " + key;
      }
   }

   private void assertNotInCacheAndStore(Object... keys) throws PersistenceException {
      assertNotInCacheAndStore(cache, store, keys);
   }

   private void assertInStoreNotInCache(Object... keys) throws PersistenceException {
      assertInStoreNotInCache(cache, store, keys);
   }

   private void assertInStoreNotInCache(Cache<?, ?> cache, CacheLoader store, Object... keys) throws PersistenceException {
      for (Object key : keys) {
         assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert store.contains(key) : "Store should contain key " + key;
      }
   }

   private void assertInCacheAndNotInStore(Object... keys) throws PersistenceException {
      assertInCacheAndNotInStore(cache, store, keys);
   }

   private void assertInCacheAndNotInStore(Cache<?, ?> cache, CacheLoader store, Object... keys) throws PersistenceException {
      for (Object key : keys) {
         assert cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert !store.contains(key) : "Store should contain key " + key;
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

      log.info("cache.get(\"k1\") = " + cache.get("k1"));
      assert cache.remove("k1", "v1");
      log.info("cache.get(\"k1\") = " + cache.get("k1"));
      assert cache.remove("k2").equals("v2");

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
      for (int i = 1; i < 5; i++) writer.write(new MarshalledEntryImpl("k" + i, "v" + i, null, sm));
      for (int i = 1; i < 5; i++) assert cache.get("k" + i).equals("v" + i);
      // make sure we have no stale locks!!
      assertNoLocks(cache);

      for (int i = 1; i < 5; i++) cache.evict("k" + i);
      // make sure we have no stale locks!!
      assertNoLocks(cache);

      assert cache.putIfAbsent("k1", "v1-SHOULD-NOT-STORE").equals("v1");
      assert cache.remove("k2").equals("v2");
      assert cache.replace("k3", "v3-REPLACED").equals("v3");
      assert cache.replace("k4", "v4", "v4-REPLACED");
      // make sure we have no stale locks!!
      assertNoLocks(cache);

      assert cache.size() == 3 : "Expected the cache to contain 3 elements but contained " + cache.entrySet();

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
      ConfigurationBuilder preloadingCfg = new ConfigurationBuilder();
      preloadingCfg.read(cfg.build());
      preloadingCfg.persistence().clearStores().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).storeName(this.getClass().getName() + "preloadingCache");
      doPreloadingTest(preloadingCfg.build(), "preloadingCache");
   }

   public void testPreloadingWithoutAutoCommit() throws Exception {
      ConfigurationBuilder preloadingCfg = new ConfigurationBuilder();
      preloadingCfg.read(cfg.build());
      preloadingCfg.persistence().clearStores().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).storeName(this.getClass().getName() + "preloadingCache_2");
      preloadingCfg.transaction().autoCommit(false);
      doPreloadingTest(preloadingCfg.build(), "preloadingCache_2");
   }

   public void testPreloadingWithEvictionAndOneMaxEntry() throws Exception {
      ConfigurationBuilder preloadingCfg = new ConfigurationBuilder();
      preloadingCfg.read(cfg.build());
      preloadingCfg.persistence().clearStores().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).storeName(this.getClass().getName() + "preloadingCache_3");
      preloadingCfg.eviction().strategy(EvictionStrategy.LIRS).maxEntries(1);
      doPreloadingTestWithEviction(preloadingCfg.build(), "preloadingCache_3");
   }

   public void testPreloadingWithEviction() throws Exception {
      ConfigurationBuilder preloadingCfg = new ConfigurationBuilder();
      preloadingCfg.read(cfg.build());
      preloadingCfg.persistence().clearStores().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).storeName(this.getClass().getName() + "preloadingCache_4");
      preloadingCfg.eviction().strategy(EvictionStrategy.LIRS).maxEntries(3);
      doPreloadingTestWithEviction(preloadingCfg.build(), "preloadingCache_4");
   }

   @Test(groups = "unstable")
   public void testPurgeOnStartup() throws PersistenceException {
      ConfigurationBuilder purgingCfg = new ConfigurationBuilder();
      purgingCfg.read(cfg.build());
      purgingCfg.persistence().clearStores().addStore(DummyInMemoryStoreConfigurationBuilder.class)
         .storeName("purgingCache").purgeOnStartup(true);
      cm.defineConfiguration("purgingCache", purgingCfg.build());
      Cache<String, String> purgingCache = cm.getCache("purgingCache");
      AdvancedCacheLoader purgingLoader = (AdvancedCacheLoader) TestingUtil.getCacheLoader(purgingCache);

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
      assert c.size() == 4;
      purgingCache.stop();
      assert c.size() == 0;

      purgingCache.start();
      purgingLoader = (AdvancedCacheLoader) TestingUtil.getCacheLoader(purgingCache);
      c = purgingCache.getAdvancedCache().getDataContainer();
      assert c.size() == 0;

      assertNotInCacheAndStore(purgingCache, purgingLoader, "k1", "k2", "k3", "k4");
   }

   public void testTransactionalWrites() throws Exception {
      assert cache.getStatus() == ComponentStatus.RUNNING;
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
      cache.clear();
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
      cache.clear();
      t = tm.suspend();

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2", lifespan);
      tm.resume(t);
      tm.rollback();

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2", lifespan);
   }

   public void testTransactionalReplace(Method m) throws Exception {
      assert cache.getStatus() == ComponentStatus.RUNNING;
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

      assert "v1".equals(cache.remove("k1"));
      assert "v2".equals(cache.remove("k2"));
   }

   public void testLoadingToMemory() throws PersistenceException {
      assertNotInCacheAndStore("k1", "k2");
      store.write(new MarshalledEntryImpl("k1", "v1", null, sm));
      store.write(new MarshalledEntryImpl("k2", "v2", null, sm));

      assertInStoreNotInCache("k1", "k2");

      assert "v1".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2");

      store.delete("k1");
      store.delete("k2");

      assertInCacheAndNotInStore("k1", "k2");
      assert "v1".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));
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
      assert value.equals(cache.get(key));
      cache.stop();
      cache.start();
      tm.begin();
      cache.containsKey(key); // Necessary call to force locks being acquired in advance
      cache.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key);
      cache.put(key, value);
      tm.commit();
      assert value.equals(cache.get(key));
   }

   public void testNullFoundButLoaderReceivedValueLaterInTransaction() throws SystemException, NotSupportedException {
      assertNotInCacheAndStore("k1");
      tm.begin();
      try {
         assertNull(cache.get("k1"));

         // Now simulate that someone else wrote to the store while during our tx
         store.write(new MarshalledEntryImpl("k1", "v1", null, sm));
         IsolationLevel level = cache.getCacheConfiguration().locking().isolationLevel();
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

   protected void doPreloadingTest(Configuration preloadingCfg, String cacheName) throws Exception {
      assertTrue("Preload not enabled for preload test", preloadingCfg.persistence().preload());
      cm.defineConfiguration(cacheName, preloadingCfg);
      Cache<String, String> preloadingCache = cm.getCache(cacheName);
      AdvancedCacheLoader preloadingCacheLoader = (AdvancedCacheLoader) TestingUtil.getCacheLoader(preloadingCache);

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
      assert c.size() == 4;
      preloadingCache.stop();
      assert c.size() == 0;

      preloadingCache.start();
      // The old store's marshaller is not working any more
      preloadingCacheLoader = (AdvancedCacheLoader) TestingUtil.getCacheLoader(preloadingCache);
      assert preloadingCache.getCacheConfiguration().persistence().preload();
      c = preloadingCache.getAdvancedCache().getDataContainer();
      assert c.size() == 4;

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(preloadingCache, preloadingCacheLoader, "k" + i, "v" + i);
         else
            assertInCacheAndStore(preloadingCache, preloadingCacheLoader, "k" + i, "v" + i, lifespan);
      }
   }

   protected void doPreloadingTestWithEviction(Configuration preloadingCfg, String cacheName) throws Exception {
      assertTrue("Preload not enabled for preload with eviction test", preloadingCfg.persistence().preload());
      assertTrue("Eviction not enabled for preload with eviction test", preloadingCfg.eviction().strategy().isEnabled());

      cm.defineConfiguration(cacheName, preloadingCfg);

      final Cache<String, String> preloadingCache = cm.getCache(cacheName);
      final int expectedEntriesInContainer = Math.min(4, preloadingCfg.eviction().maxEntries());
      AdvancedCacheLoader preloadingCacheLoader = (AdvancedCacheLoader) TestingUtil.getCacheLoader(preloadingCache);

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
         final Object key = "k" + i;
         final Object value = "v" + i;
         final long lifespan = i % 2 == 1 ? -1 : this.lifespan;
         boolean found = false;
         InternalCacheEntry se = preloadingCache.getAdvancedCache().getDataContainer().get(key);
         MarshalledEntry load = preloadingCacheLoader.load(key);
         if (se != null) {
            testStoredEntry(se.getValue(), value, se.getLifespan(), lifespan, "Cache", key);
            found = true;
         }
         if (load != null) {
            testStoredEntry(load.getValue(), value, load.getMetadata().lifespan(), lifespan, "Store", key);
            found = true;
         }
         assertTrue("Key not found.", found);
      }

      preloadingCache.stop();
      assertEquals("DataContainer still has entries after stop", 0, c.size());

      preloadingCache.start();
      // The old store's marshaller is not working any more
      preloadingCacheLoader = (AdvancedCacheLoader) TestingUtil.getCacheLoader(preloadingCache);

      assertTrue("Preload not enabled in cache configuration",
                 preloadingCache.getCacheConfiguration().persistence().preload());

      c = preloadingCache.getAdvancedCache().getDataContainer();
      assertEquals("Wrong number of entries in data container", expectedEntriesInContainer, c.size());

      for (int i = 1; i < 5; i++) {
         final Object key = "k" + i;
         final Object value = "v" + i;
         final long lifespan = i % 2 == 1 ? -1 : this.lifespan;
         boolean found = false;
         InternalCacheEntry se = preloadingCache.getAdvancedCache().getDataContainer().get(key);
         MarshalledEntry load = preloadingCacheLoader.load(key);
         if (se != null) {
            testStoredEntry(se.getValue(), value, se.getLifespan(), lifespan, "Cache", key);
            found = true;
         }
         if (load != null) {
            testStoredEntry(load.getValue(), value, load.getMetadata().lifespan(), lifespan, "Store", key);
            found = true;
         }
         assertTrue("Key not found.", found);
      }
   }

}
