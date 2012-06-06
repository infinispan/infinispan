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
package org.infinispan.loaders.decorators;

import org.infinispan.CacheException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

@Test(groups = "unit", testName = "loaders.decorators.AsyncTest", sequential=true)
public class AsyncTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(AsyncTest.class);
   AsyncStore store;
   ExecutorService asyncExecutor;
   DummyInMemoryCacheStore underlying;
   AsyncStoreConfig asyncConfig;
   DummyInMemoryCacheStore.Cfg dummyCfg;

   @BeforeMethod
   public void setUp() throws CacheLoaderException {
      underlying = new DummyInMemoryCacheStore();
      asyncConfig = new AsyncStoreConfig().threadPoolSize(10);
      store = new AsyncStore(underlying, asyncConfig);
      dummyCfg = new DummyInMemoryCacheStore.Cfg().storeName(AsyncTest.class.getName());
      store.init(dummyCfg, null, null);
      store.start();
      asyncExecutor = (ExecutorService) TestingUtil.extractField(store, "executor");
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() throws CacheLoaderException {
      if (store != null) store.stop();
   }

   @Test(timeOut=10000)
   public void testPutRemove() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      final int number = 1000;
      String key = "testPutRemove-k-";
      String value = "testPutRemove-v-";
      doTestPut(number, key, value);
      doTestRemove(number, key);
   }

   @Test(timeOut=10000)
   public void testPutClearPut() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      final int number = 1000;
      String key = "testPutClearPut-k-";
      String value = "testPutClearPut-v-";
      doTestPut(number, key, value);
      doTestClear(number, key);
      value = "testPutClearPut-v[2]-";
      doTestPut(number, key, value);
      doTestRemove(number, key);
   }

   @Test(timeOut=10000)
   public void testMultiplePutsOnSameKey() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      final int number = 1000;
      String key = "testMultiplePutsOnSameKey-k";
      String value = "testMultiplePutsOnSameKey-v-";
      doTestSameKeyPut(number, key, value);
      doTestSameKeyRemove(key);
   }

   @Test(timeOut=10000)
   public void testRestrictionOnAddingToAsyncQueue() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      store.remove("blah");

      final int number = 10;
      String key = "testRestrictionOnAddingToAsyncQueue-k";
      String value = "testRestrictionOnAddingToAsyncQueue-v-";
      doTestPut(number, key, value);

      // stop the cache store
      store.stop();
      try {
         store.remove("blah");
         assert false : "Should have restricted this entry from being made";
      }
      catch (CacheException expected) {
      }

      // clean up
      store.start();
      doTestRemove(number, key);
   }

   public void testThreadSafetyWritingDiffValuesForKey(Method m) throws Exception {
      try {
         final String key = "k1";
         final CountDownLatch v1Latch = new CountDownLatch(1);
         final CountDownLatch v2Latch = new CountDownLatch(1);
         final CountDownLatch endLatch = new CountDownLatch(1);
         DummyInMemoryCacheStore underlying = new DummyInMemoryCacheStore();
         store = new MockAsyncStore(key, v1Latch, v2Latch, endLatch, underlying, asyncConfig);
         dummyCfg = new DummyInMemoryCacheStore.Cfg();
         dummyCfg.setStoreName(m.getName());
         store.init(dummyCfg, null, null);
         store.start();

         store.store(TestInternalCacheEntryFactory.create(key, "v1"));
         v2Latch.await();
         store.store(TestInternalCacheEntryFactory.create(key, "v2"));
         endLatch.await();

         assert store.load(key).getValue().equals("v2");
      } finally {
         store.delegate.clear();
         store.stop();
         store = null;
      }
   }

   public void testTransactionalModificationsHappenInDiffThread(Method m) throws Exception {
      try {
         final TransactionFactory gtf = new TransactionFactory();
         gtf.init(false, false, true);
         final String k1 = k(m, 1), k2 = k(m, 2), v1 = v(m, 1), v2 = v(m, 2);
         final ConcurrentMap<Object, Modification> localMods = new ConcurrentHashMap<Object, Modification>();
         final CyclicBarrier barrier = new CyclicBarrier(2);
         DummyInMemoryCacheStore underlying = new DummyInMemoryCacheStore();
         store = new AsyncStore(underlying, asyncConfig) {
            @Override
            protected void applyModificationsSync(ConcurrentMap<Object, Modification> mods) throws CacheLoaderException {
               for (Map.Entry<Object, Modification> entry : mods.entrySet()) {
                  localMods.put(entry.getKey(), entry.getValue());
               }
               super.applyModificationsSync(mods);
               try {
                  barrier.await(5, TimeUnit.SECONDS);
               } catch (TimeoutException e) {
                  assert false : "Timed out applying for modifications";
               } catch (Exception e) {
                  throw new CacheLoaderException("Barrier failed", e);
               }
            }
         };
         dummyCfg = new DummyInMemoryCacheStore.Cfg();
         dummyCfg.setStoreName(m.getName());
         store.init(dummyCfg, null, null);
         store.start();

         List<Modification> mods = new ArrayList<Modification>();
         mods.add(new Store(TestInternalCacheEntryFactory.create(k1, v1)));
         mods.add(new Store(TestInternalCacheEntryFactory.create(k2, v2)));
         mods.add(new Remove(k1));
         GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
         store.prepare(mods, tx, false);

         assert 0 == localMods.size();
         assert !store.containsKey(k1);
         assert !store.containsKey(k2);

         store.commit(tx);
         barrier.await(5, TimeUnit.SECONDS);
         assert store.load(k2).getValue().equals(v2);
         assert !store.containsKey(k1);
         assert 2 == localMods.size();
         assert new Remove(k1).equals(localMods.get(k1));
      } finally {
         store.delegate.clear();
         store.stop();
         store = null;
      }
   }

   public void testTransactionalModificationsAreCoalesced(Method m) throws Exception {
      try {
         final TransactionFactory gtf = new TransactionFactory();
         gtf.init(false, false, true);
         final String k1 = k(m, 1), k2 = k(m, 2), k3 = k(m, 3), v1 = v(m, 1), v2 = v(m, 2), v3 = v(m, 3);
         final AtomicInteger storeCount = new AtomicInteger();
         final AtomicInteger removeCount = new AtomicInteger();
         final AtomicInteger clearCount = new AtomicInteger();
         final CyclicBarrier barrier = new CyclicBarrier(2);
         DummyInMemoryCacheStore underlying = new DummyInMemoryCacheStore() {
            @Override
            public void store(InternalCacheEntry ed) {
               super.store(ed);
               storeCount.getAndIncrement();
            }

            @Override
            public boolean remove(Object key) {
               boolean ret = super.remove(key);
               removeCount.getAndIncrement();
               return ret;
            }

            @Override
            public void clear() {
               super.clear();
               clearCount.getAndIncrement();
            }
         };
         store = new AsyncStore(underlying, asyncConfig) {
            @Override
            protected void applyModificationsSync(ConcurrentMap<Object, Modification> mods) throws CacheLoaderException {
               super.applyModificationsSync(mods);
               try {
                  barrier.await(5, TimeUnit.SECONDS);
               } catch (TimeoutException e) {
                  assert false : "Timed out applying for modifications";
               } catch (Exception e) {
                  throw new CacheLoaderException("Barrier failed", e);
               }
            }
         };
         dummyCfg = new DummyInMemoryCacheStore.Cfg();
         dummyCfg.setStoreName(m.getName());
         store.init(dummyCfg, null, null);
         store.start();

         List<Modification> mods = new ArrayList<Modification>();
         mods.add(new Store(TestInternalCacheEntryFactory.create(k1, v1)));
         mods.add(new Store(TestInternalCacheEntryFactory.create(k1, v2)));
         mods.add(new Store(TestInternalCacheEntryFactory.create(k2, v1)));
         mods.add(new Store(TestInternalCacheEntryFactory.create(k2, v2)));
         mods.add(new Remove(k1));
         GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
         store.prepare(mods, tx, false);
         Thread.sleep(200); //verify that work is not performed until commit
         assert 0 == storeCount.get();
         assert 0 == removeCount.get();
         assert 0 == clearCount.get();
         store.commit(tx);
         barrier.await(5, TimeUnit.SECONDS); //modifications applied all at once
         assert 1 == storeCount.get() : "Store count was " + storeCount.get();
         assert 1 == removeCount.get();
         assert 0 == clearCount.get();

         storeCount.set(0);
         removeCount.set(0);
         clearCount.set(0);
         mods = new ArrayList<Modification>();
         mods.add(new Store(TestInternalCacheEntryFactory.create(k1, v1)));
         mods.add(new Remove(k1));
         mods.add(new Clear());
         mods.add(new Store(TestInternalCacheEntryFactory.create(k2, v2)));
         mods.add(new Remove(k2));
         tx = gtf.newGlobalTransaction(null, false);
         store.prepare(mods, tx, false);
         Thread.sleep(200); //verify that work is not performed until commit
         assert 0 == storeCount.get();
         assert 0 == removeCount.get();
         assert 0 == clearCount.get();
         store.commit(tx);
         barrier.await(5, TimeUnit.SECONDS);
         assert 0 == storeCount.get() : "Store count was " + storeCount.get();
         assert 1 == removeCount.get();
         assert 1 == clearCount.get();

         storeCount.set(0);
         removeCount.set(0);
         clearCount.set(0);
         mods = new ArrayList<Modification>();
         mods.add(new Store(TestInternalCacheEntryFactory.create(k1, v1)));
         mods.add(new Remove(k1));
         mods.add(new Store(TestInternalCacheEntryFactory.create(k2, v2)));
         mods.add(new Remove(k2));
         mods.add(new Store(TestInternalCacheEntryFactory.create(k3, v3)));
         tx = gtf.newGlobalTransaction(null, false);
         store.prepare(mods, tx, false);
         Thread.sleep(200);
         assert 0 == storeCount.get();
         assert 0 == removeCount.get();
         assert 0 == clearCount.get();
         store.commit(tx);
         barrier.await(5, TimeUnit.SECONDS);
         assert 1 == storeCount.get() : "Store count was " + storeCount.get();
         assert 2 == removeCount.get();
         assert 0 == clearCount.get();

         storeCount.set(0);
         removeCount.set(0);
         clearCount.set(0);
         mods = new ArrayList<Modification>();
         mods.add(new Clear());
         mods.add(new Remove(k1));
         tx = gtf.newGlobalTransaction(null, false);
         store.prepare(mods, tx, false);
         Thread.sleep(200);
         assert 0 == storeCount.get();
         assert 0 == removeCount.get();
         assert 0 == clearCount.get();
         store.commit(tx);
         barrier.await(5, TimeUnit.SECONDS);
         assert 0 == storeCount.get() : "Store count was " + storeCount.get();
         assert 1 == removeCount.get();
         assert 1 == clearCount.get();

         storeCount.set(0);
         removeCount.set(0);
         clearCount.set(0);
         mods = new ArrayList<Modification>();
         mods.add(new Clear());
         mods.add(new Store(TestInternalCacheEntryFactory.create(k1, v1)));
         tx = gtf.newGlobalTransaction(null, false);
         store.prepare(mods, tx, false);
         Thread.sleep(200);
         assert 0 == storeCount.get();
         assert 0 == removeCount.get();
         assert 0 == clearCount.get();
         store.commit(tx);
         barrier.await(5, TimeUnit.SECONDS);
         assert 1 == storeCount.get() : "Store count was " + storeCount.get();
         assert 0 == removeCount.get();
         assert 1 == clearCount.get();
      } finally {
         store.delegate.clear();
         store.stop();
         store = null;
      }
   }

   private void doTestPut(int number, String key, String value) throws Exception {
      for (int i = 0; i < number; i++) {
         InternalCacheEntry cacheEntry = TestInternalCacheEntryFactory.create(key + i, value + i);
         store.store(cacheEntry);
      }

      store.stop();
      store.start();

      InternalCacheEntry[] entries = new InternalCacheEntry[number];
      for (int i = 0; i < number; i++) {
         entries[i] = store.load(key + i);
      }

      for (int i = 0; i < number; i++) {
         InternalCacheEntry entry = entries[i];
         if (entry != null) {
            assert entry.getValue().equals(value + i);
         } else {
            while (entry == null) {
               entry = store.load(key + i);
               if (entry != null) {
                  assert entry.getValue().equals(value + i);
               } else {
                  TestingUtil.sleepThreadInt(20, "still waiting for key to appear: " + key + i);
               }
            }
         }
      }
   }

   private void doTestSameKeyPut(int number, String key, String value) throws Exception {
      for (int i = 0; i < number; i++) {
         store.store(TestInternalCacheEntryFactory.create(key, value + i));
      }

      store.stop();
      store.start();
      InternalCacheEntry entry;
      boolean success = false;
      for (int i = 0; i < 120; i++) {
         TestingUtil.sleepThreadInt(20, null);
         entry = store.load(key);
         success = entry.getValue().equals(value + (number - 1));
         if (success) break;
      }
      assert success;
   }

   private void doTestRemove(int number, String key) throws Exception {
      for (int i = 0; i < number; i++) store.remove(key + i);

      store.stop();//makes sure the store is flushed
      store.start();

      InternalCacheEntry[] entries = new InternalCacheEntry[number];
      for (int i = 0; i < number; i++) {
         entries[i] = store.load(key + i);
      }

      for (int i = 0; i < number; i++) {
         InternalCacheEntry entry = entries[i];
         while (entry != null) {
            TestingUtil.sleepThreadInt(20, "still waiting for key to be removed: " + key + i);
            entry = store.load(key + i);
         }
      }
   }

   private void doTestSameKeyRemove(String key) throws Exception {
      store.remove(key);
      InternalCacheEntry entry;
      do {
         TestingUtil.sleepThreadInt(20, "still waiting for key to be removed: " + key);
         entry = store.load(key);
      } while (entry != null);
   }

   private void doTestClear(int number, String key) throws Exception {
      store.clear();
      store.stop();
      store.start();

      InternalCacheEntry[] entries = new InternalCacheEntry[number];
      for (int i = 0; i < number; i++) {
         entries[i] = store.load(key + i);
      }

      for (int i = 0; i < number; i++) {
         InternalCacheEntry entry = entries[i];
         while (entry != null) {
            TestingUtil.sleepThreadInt(20, "still waiting for key to be removed: " + key + i);
            entry = store.load(key + i);
         }
      }
   }

   static class MockAsyncStore extends AsyncStore {
      volatile boolean block = true;
      final CountDownLatch v1Latch;
      final CountDownLatch v2Latch;
      final CountDownLatch endLatch;
      final Object key;

      MockAsyncStore(Object key, CountDownLatch v1Latch, CountDownLatch v2Latch, CountDownLatch endLatch,
                     CacheStore delegate, AsyncStoreConfig asyncStoreConfig) {
         super(delegate, asyncStoreConfig);
         this.v1Latch = v1Latch;
         this.v2Latch = v2Latch;
         this.endLatch = endLatch;
         this.key = key;
      }

      @Override
      protected void applyModificationsSync(ConcurrentMap<Object, Modification> mods) throws CacheLoaderException {
         if (mods.get(key) != null && block) {
            log.trace("Wait for v1 latch");
            try {
               v2Latch.countDown();
               block = false;
               v1Latch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            super.applyModificationsSync(mods);
         } else if (mods.get(key) != null && !block) {
            log.trace("Do v2 modification and unleash v1 latch");
            super.applyModificationsSync(mods);
            v1Latch.countDown();
            endLatch.countDown();
         }
      }

   }
}
