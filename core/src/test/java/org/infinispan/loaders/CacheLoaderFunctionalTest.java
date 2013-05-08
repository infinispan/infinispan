/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.lang.reflect.Method;
import java.util.Collections;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.api.mvcc.LockAssert.assertNoLocks;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

/**
 * Tests the interceptor chain and surrounding logic
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "loaders.CacheLoaderFunctionalTest")
public class CacheLoaderFunctionalTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(CacheLoaderFunctionalTest.class);

   Cache cache;
   CacheStore store;
   TransactionManager tm;
   Configuration cfg;
   EmbeddedCacheManager cm;
   long lifespan = 60000000; // very large lifespan so nothing actually expires

   @BeforeMethod
   public void setUp() {
      cfg = new Configuration().fluent()
         .loaders()
            .addCacheLoader(new DummyInMemoryCacheStore.Cfg()
               .storeName(this.getClass().getName())) // in order to use the same store
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
         .build();
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      store.clear();

      TestingUtil.killCacheManagers(cm);
      cache = null;
      cm = null;
      cfg = null;
      tm = null;
      store = null;
   }

   private void assertInCacheAndStore(Object key, Object value) throws CacheLoaderException {
      assertInCacheAndStore(key, value, -1);
   }

   private void assertInCacheAndStore(Object key, Object value, long lifespanMillis) throws CacheLoaderException {
      assertInCacheAndStore(cache, store, key, value, lifespanMillis);
   }


   private void assertInCacheAndStore(Cache cache, CacheStore store, Object key, Object value) throws CacheLoaderException {
      assertInCacheAndStore(cache, store, key, value, -1);
   }

   private void assertInCacheAndStore(Cache cache, CacheStore store, Object key, Object value, long lifespanMillis) throws CacheLoaderException {
      InternalCacheEntry se = cache.getAdvancedCache().getDataContainer().get(key);
      testStoredEntry(se, value, lifespanMillis, "Cache", key);
      se = store.load(key);
      testStoredEntry(se, value, lifespanMillis, "Store", key);
   }

   private void testStoredEntry(InternalCacheEntry entry, Object expectedValue, long expectedLifespan, String src, Object key) {
      assert entry != null : src + " entry for key " + key + " should NOT be null";
      assert entry.getValue().equals(expectedValue) : src + " should contain value " + expectedValue + " under key " + entry.getKey() + " but was " + entry.getValue() + ". Entry is " + entry;
      assert entry.getLifespan() == expectedLifespan : src + " expected lifespan for key " + key + " to be " + expectedLifespan + " but was " + entry.getLifespan() + ". Entry is " + entry;
   }

   private void assertNotInCacheAndStore(Cache cache, CacheStore store, Object... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert !store.containsKey(key) : "Store should not contain key " + key;
      }
   }

   private void assertNotInCacheAndStore(Object... keys) throws CacheLoaderException {
      assertNotInCacheAndStore(cache, store, keys);
   }

   private void assertInStoreNotInCache(Object... keys) throws CacheLoaderException {
      assertInStoreNotInCache(cache, store, keys);
   }

   private void assertInStoreNotInCache(Cache cache, CacheStore store, Object... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assert !cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert store.containsKey(key) : "Store should contain key " + key;
      }
   }

   private void assertInCacheAndNotInStore(Object... keys) throws CacheLoaderException {
      assertInCacheAndNotInStore(cache, store, keys);
   }

   private void assertInCacheAndNotInStore(Cache cache, CacheStore store, Object... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assert cache.getAdvancedCache().getDataContainer().containsKey(key) : "Cache should not contain key " + key;
         assert !store.containsKey(key) : "Store should contain key " + key;
      }
   }


   public void testStoreAndRetrieve() throws CacheLoaderException {
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

   public void testReplaceMethods() throws CacheLoaderException {
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

   public void testLoading() throws CacheLoaderException {
      assertNotInCacheAndStore("k1", "k2", "k3", "k4");
      for (int i = 1; i < 5; i++) store.store(TestInternalCacheEntryFactory.create("k" + i, "v" + i));
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

      assert cache.isEmpty(); // cache size ops will not trigger a load

      cache.clear(); // this should propagate to the loader though
      assertNotInCacheAndStore("k1", "k2", "k3", "k4");
      // make sure we have no stale locks!!
      assertNoLocks(cache);
   }

   public void testPreloading() throws CacheLoaderException {
      Configuration preloadingCfg = cfg.clone();
      preloadingCfg.getCacheLoaderManagerConfig().setPreload(true);
      ((DummyInMemoryCacheStore.Cfg) preloadingCfg.getCacheLoaderManagerConfig().getFirstCacheLoaderConfig()).setStoreName("preloadingCache");
      cm.defineConfiguration("preloadingCache", preloadingCfg);
      Cache preloadingCache = cm.getCache("preloadingCache");
      CacheStore preloadingStore = TestingUtil.extractComponent(preloadingCache, CacheLoaderManager.class).getCacheStore();

      assert preloadingCache.getConfiguration().getCacheLoaderManagerConfig().isPreload();

      assertNotInCacheAndStore(preloadingCache, preloadingStore, "k1", "k2", "k3", "k4");

      preloadingCache.put("k1", "v1");
      preloadingCache.put("k2", "v2", lifespan, MILLISECONDS);
      preloadingCache.put("k3", "v3");
      preloadingCache.put("k4", "v4", lifespan, MILLISECONDS);

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(preloadingCache, preloadingStore, "k" + i, "v" + i);
         else
            assertInCacheAndStore(preloadingCache, preloadingStore, "k" + i, "v" + i, lifespan);
      }

      DataContainer c = preloadingCache.getAdvancedCache().getDataContainer();
      assert c.size() == 4;
      preloadingCache.stop();
      assert c.size() == 0;

      preloadingCache.start();
      // The old store's marshaller is not working any more
      preloadingStore = TestingUtil.extractComponent(preloadingCache, CacheLoaderManager.class).getCacheStore();
      assert preloadingCache.getConfiguration().getCacheLoaderManagerConfig().isPreload();
      c = preloadingCache.getAdvancedCache().getDataContainer();
      assert c.size() == 4;

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(preloadingCache, preloadingStore, "k" + i, "v" + i);
         else
            assertInCacheAndStore(preloadingCache, preloadingStore, "k" + i, "v" + i, lifespan);
      }
   }

   public void testPurgeOnStartup() throws CacheLoaderException {
      Configuration purgingCfg = cfg.clone();
      CacheStoreConfig firstCacheLoaderConfig = (CacheStoreConfig) purgingCfg.getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
      firstCacheLoaderConfig.setPurgeOnStartup(true);
      ((DummyInMemoryCacheStore.Cfg) purgingCfg.getCacheLoaderManagerConfig().getFirstCacheLoaderConfig()).setStoreName("purgingCache");
      cm.defineConfiguration("purgingCache", purgingCfg);
      Cache purgingCache = cm.getCache("purgingCache");
      CacheStore purgingStore = TestingUtil.extractComponent(purgingCache, CacheLoaderManager.class).getCacheStore();

      assertNotInCacheAndStore(purgingCache, purgingStore, "k1", "k2", "k3", "k4");

      purgingCache.put("k1", "v1");
      purgingCache.put("k2", "v2", lifespan, MILLISECONDS);
      purgingCache.put("k3", "v3");
      purgingCache.put("k4", "v4", lifespan, MILLISECONDS);

      for (int i = 1; i < 5; i++) {
         if (i % 2 == 1)
            assertInCacheAndStore(purgingCache, purgingStore, "k" + i, "v" + i);
         else
            assertInCacheAndStore(purgingCache, purgingStore, "k" + i, "v" + i, lifespan);
      }

      DataContainer c = purgingCache.getAdvancedCache().getDataContainer();
      assert c.size() == 4;
      purgingCache.stop();
      assert c.size() == 0;

      purgingCache.start();
      c = purgingCache.getAdvancedCache().getDataContainer();
      assert c.size() == 0;

      assertNotInCacheAndStore(purgingCache, purgingStore, "k1", "k2", "k3", "k4");
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

   public void testEvictAndRemove() throws CacheLoaderException {
      assertNotInCacheAndStore("k1", "k2");
      cache.put("k1", "v1");
      cache.put("k2", "v2", lifespan, MILLISECONDS);

      cache.evict("k1");
      cache.evict("k2");

      assert "v1".equals(cache.remove("k1"));
      assert "v2".equals(cache.remove("k2"));
   }

   public void testLoadingToMemory() throws CacheLoaderException {
      assertNotInCacheAndStore("k1", "k2");
      store.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      store.store(TestInternalCacheEntryFactory.create("k2", "v2"));

      assertInStoreNotInCache("k1", "k2");

      assert "v1".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));

      assertInCacheAndStore("k1", "v1");
      assertInCacheAndStore("k2", "v2");

      store.remove("k1");
      store.remove("k2");

      assertInCacheAndNotInStore("k1", "k2");
      assert "v1".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));
   }

   public void testSkipLocking(Method m) {
      String name = m.getName();
      AdvancedCache advancedCache = cache.getAdvancedCache();
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

   public void testGetCacheLoadersFromConfigAfterStart() {
      cache.getConfiguration().getCacheLoaders();
      cache.getConfiguration().getCacheLoaders();
   }

}
