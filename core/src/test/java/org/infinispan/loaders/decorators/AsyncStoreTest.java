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

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStoreTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

@Test(groups = "unit", testName = "loaders.decorators.AsyncStoreTest", sequential=true)
public class AsyncStoreTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(AsyncStoreTest.class);
   private AsyncStore store;

   private void createStore() throws CacheLoaderException {
      DummyInMemoryCacheStore underlying = new DummyInMemoryCacheStore();
      AsyncStoreConfig asyncConfig = new AsyncStoreConfig().threadPoolSize(10);
      store = new AsyncStore(underlying, asyncConfig);
      DummyInMemoryCacheStore.Cfg dummyCfg = new DummyInMemoryCacheStore.Cfg().storeName(AsyncStoreTest.class.getName());
      store.init(dummyCfg, getCache(), null);
      store.start();
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      if (store != null) store.stop();
   }

   @Test(timeOut=10000)
   public void testPutRemove() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      createStore();

      final int number = 1000;
      String key = "testPutRemove-k-";
      String value = "testPutRemove-v-";
      doTestPut(number, key, value);
      doTestRemove(number, key);
   }

   @Test(timeOut=10000)
   public void testPutClearPut() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      createStore();

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
      createStore();

      final int number = 1000;
      String key = "testMultiplePutsOnSameKey-k";
      String value = "testMultiplePutsOnSameKey-v-";
      doTestSameKeyPut(number, key, value);
      doTestSameKeyRemove(key);
   }

   @Test(timeOut=10000)
   public void testRestrictionOnAddingToAsyncQueue() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      createStore();

      store.remove("blah");

      final int number = 10;
      String key = "testRestrictionOnAddingToAsyncQueue-k";
      String value = "testRestrictionOnAddingToAsyncQueue-v-";
      doTestPut(number, key, value);

      // stop the cache store
      store.stop();
      try {
         store.store(null);
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
         AsyncStoreConfig asyncConfig = new AsyncStoreConfig().threadPoolSize(10);
         store = new MockAsyncStore(key, v1Latch, v2Latch, endLatch, underlying, asyncConfig);
         DummyInMemoryCacheStore.Cfg dummyCfg = new DummyInMemoryCacheStore.Cfg();
         dummyCfg.storeName(m.getName());
         store.init(dummyCfg, getCache(), null);
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
      final int waitTimeout = 10;
      final TimeUnit waitUnit = TimeUnit.SECONDS;
      try {
         final TransactionFactory gtf = new TransactionFactory();
         gtf.init(false, false, true, false);
         final String k1 = k(m, 1), k2 = k(m, 2), v1 = v(m, 1), v2 = v(m, 2);
         final ConcurrentMap<Object, Modification> localMods = new ConcurrentHashMap<Object, Modification>();
         final CyclicBarrier barrier = new CyclicBarrier(2);
         DummyInMemoryCacheStore underlying = new DummyInMemoryCacheStore();
         AsyncStoreConfig asyncConfig = new AsyncStoreConfig().threadPoolSize(10);
         store = new AsyncStore(underlying, asyncConfig) {
            @Override
            protected void applyModificationsSync(List<Modification> mods) throws CacheLoaderException {
               for (Modification mod : mods)
                  localMods.put(getKey(mod), mod);

               super.applyModificationsSync(mods);
               try {
                  barrier.await(waitTimeout, waitUnit);
               } catch (TimeoutException e) {
                  assert false : "Timed out applying for modifications";
               } catch (Exception e) {
                  throw new CacheLoaderException("Barrier failed", e);
               }
            }

            private Object getKey(Modification modification) {
               switch (modification.getType()) {
                  case STORE:
                     return ((Store) modification).getStoredEntry().getKey();
                  case REMOVE:
                     return ((Remove) modification).getKey();
                  default:
                     return null;
               }
            }
         };
         DummyInMemoryCacheStore.Cfg dummyCfg = new DummyInMemoryCacheStore.Cfg();
         dummyCfg.storeName(m.getName());
         store.init(dummyCfg, getCache(), null);
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
         barrier.await(waitTimeout, waitUnit); // Wait for store
         barrier.await(waitTimeout, waitUnit); // Wait for remove
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
      final int waitTimeout = 10;
      final TimeUnit waitUnit = TimeUnit.SECONDS;
      try {
         final TransactionFactory gtf = new TransactionFactory();
         gtf.init(false, false, true, false);
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
         AsyncStoreConfig asyncConfig = new AsyncStoreConfig().threadPoolSize(10);
         store = new AsyncStore(underlying, asyncConfig) {
            @Override
            protected void applyModificationsSync(List<Modification> mods)
                  throws CacheLoaderException {
               super.applyModificationsSync(mods);
               try {
                  log.tracef("Wait to apply modifications: %s", mods);
                  barrier.await(waitTimeout, waitUnit);
               } catch (TimeoutException e) {
                  assert false : "Timed out applying for modifications";
               } catch (Exception e) {
                  throw new CacheLoaderException("Barrier failed", e);
               }
            }
         };
         DummyInMemoryCacheStore.Cfg dummyCfg = new DummyInMemoryCacheStore.Cfg();
         dummyCfg.storeName(m.getName());
         store.init(dummyCfg, getCache(), null);
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
         log.tracef("Wait for modifications to be queued: %s", mods);
         barrier.await(waitTimeout, waitUnit); // Wait for single store to be applied
         barrier.await(waitTimeout, waitUnit); // Wait for single remove to be applied
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
         barrier.await(waitTimeout, waitUnit);
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
         barrier.await(waitTimeout, waitUnit); // Wait for store to be applied
         barrier.await(waitTimeout, waitUnit); // Wait for first removal to be applied
         barrier.await(waitTimeout, waitUnit); // Wait for second removal to be applied
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
         barrier.await(waitTimeout, waitUnit);
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
         barrier.await(waitTimeout, waitUnit);
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

      for (int i = 0; i < number; i++) {
         InternalCacheEntry ice = store.load(key + i);
         assert ice != null && (value + i).equals(ice.getValue());
      }
   }

   private void doTestSameKeyPut(int number, String key, String value) throws Exception {
      for (int i = 0; i < number; i++) {
         store.store(TestInternalCacheEntryFactory.create(key, value + i));
      }
      InternalCacheEntry ice = store.load(key);
      assert ice != null && (value + (number - 1)).equals(ice.getValue());
   }

   private void doTestRemove(final int number, final String key) throws Exception {
      for (int i = 0; i < number; i++) store.remove(key + i);

      eventually( new Condition() {
         public boolean isSatisfied() throws Exception {
            boolean allRemoved = true;

            for (int i = 0; i < number; i++) {
               String loadKey = key + i;
               if(store.load(loadKey) != null) {
                  allRemoved = false;
                  break;
               }
            }

            return allRemoved;
         }
      });
   }

   private void doTestSameKeyRemove(String key) throws Exception {
      store.remove(key);
      assert store.load(key) == null;
   }

   private void doTestClear(int number, String key) throws Exception {
      store.clear();

      for (int i = 0; i < number; i++) {
         assert store.load(key + i) == null;
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
      protected void applyModificationsSync(List<Modification> mods) throws CacheLoaderException {
         boolean keyFound = findModificationForKey(key, mods) != null;
         if (keyFound && block) {
            log.trace("Wait for v1 latch");
            try {
               v2Latch.countDown();
               block = false;
               v1Latch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
            super.applyModificationsSync(mods);
         } else if (keyFound && !block) {
            log.trace("Do v2 modification and unleash v1 latch");
            super.applyModificationsSync(mods);
            v1Latch.countDown();
            endLatch.countDown();
         }
      }

      private Modification findModificationForKey(Object key, List<Modification> mods) {
         for (Modification modification : mods) {
            switch (modification.getType()) {
               case STORE:
                  Store store = (Store) modification;
                  if (store.getStoredEntry().getKey().equals(key))
                     return store;
                  break;
               case REMOVE:
                  Remove remove = (Remove) modification;
                  if (remove.getKey().equals(key))
                     return remove;
                  break;
               default:
                  return null;
            }
         }
         return null;
      }

   }

   private final static ThreadLocal<LockableCacheStore> STORE = new ThreadLocal<LockableCacheStore>();

   public static class LockableCacheStoreConfig extends DummyInMemoryCacheStore.Cfg {
      private static final long serialVersionUID = 1L;

      public LockableCacheStoreConfig() {
         setCacheLoaderClassName(LockableCacheStore.class.getName());
      }
   }

   @CacheLoaderMetadata(configurationClass = LockableCacheStoreConfig.class)
   public static class LockableCacheStore extends DummyInMemoryCacheStore {
      private final ReentrantLock lock = new ReentrantLock();

      public LockableCacheStore() {
         super();
         STORE.set(this);
      }

      @Override
      public Class<? extends CacheLoaderConfig> getConfigurationClass() {
         return LockableCacheStoreConfig.class;
      }

      @Override
      public void store(InternalCacheEntry ed) {
         lock.lock();
         try {
            super.store(ed);
         } finally {
            lock.unlock();
         }
      }

      @Override
      public boolean remove(Object key) {
         lock.lock();
         try {
            return super.remove(key);
         } finally {
            lock.unlock();
         }
      }
   }

   public void testModificationQueueSize(final Method m) throws Exception {
      LockableCacheStore underlying = new LockableCacheStore();
      AsyncStoreConfig asyncConfig = new AsyncStoreConfig().threadPoolSize(10);
      asyncConfig.modificationQueueSize(10);
      store = new AsyncStore(underlying, asyncConfig);
      store.init(new LockableCacheStoreConfig(), getCache(), null);
      store.start();
      try {
         final CountDownLatch done = new CountDownLatch(1);

         underlying.lock.lock();
         try {
            Thread t = new Thread() {
               @Override
               public void run() {
                  try {
                     for (int i = 0; i < 100; i++)
                        store.store(TestInternalCacheEntryFactory.create(k(m, i), v(m, i)));
                  } catch (Exception e) {
                     log.error("Error storing entry", e);
                  }
                  done.countDown();
               }
            };
            t.start();

            assert !done.await(1, TimeUnit.SECONDS) : "Background thread should have blocked after adding 10 entries";
         } finally {
            underlying.lock.unlock();
         }
      } finally {
         store.stop();
      }
   }

   private static abstract class OneEntryCacheManagerCallable extends CacheManagerCallable {
      protected final Cache<String, String> cache;
      protected final LockableCacheStore store;

      private static ConfigurationBuilder config(boolean passivation) {
         ConfigurationBuilder config = new ConfigurationBuilder();
         config.eviction().maxEntries(1).loaders().passivation(passivation).addStore()
               .cacheStore(new LockableCacheStore()).async().enable();
         return config;
      }

      OneEntryCacheManagerCallable(boolean passivation) {
         super(TestCacheManagerFactory.createCacheManager(config(passivation)));
         cache = cm.getCache();
         store = STORE.get();
      }
   }

   public void testEndToEndPutPutPassivation() throws Exception {
      doTestEndToEndPutPut(true);
   }

   public void testEndToEndPutPut() throws Exception {
      doTestEndToEndPutPut(false);
   }

   private void doTestEndToEndPutPut(boolean passivation) throws Exception {
      TestingUtil.withCacheManager(new OneEntryCacheManagerCallable(passivation) {
         @Override
         public void call() {
            cache.put("X", "1");
            cache.put("Y", "1"); // force eviction of "X"

            // wait for X == 1 to appear in store
            while (store.load("X") == null)
               TestingUtil.sleepThread(10);

            // simulate slow back end store
            store.lock.lock();
            try {
               cache.put("X", "2");
               cache.put("Y", "2"); // force eviction of "X"

               assert "2".equals(cache.get("X")) : "cache must return X == 2";
            } finally {
               store.lock.unlock();
            }
         }
      });
   }

   public void testEndToEndPutRemovePassivation() throws Exception {
      doTestEndToEndPutRemove(true);
   }

   public void testEndToEndPutRemove() throws Exception {
      doTestEndToEndPutRemove(false);
   }

   private void doTestEndToEndPutRemove(boolean passivation) throws Exception {
      TestingUtil.withCacheManager(new OneEntryCacheManagerCallable(passivation) {
         @Override
         public void call() {
            cache.put("X", "1");
            cache.put("Y", "1"); // force eviction of "X"

            // wait for "X" to appear in store
            while (store.load("X") == null)
               TestingUtil.sleepThread(10);

            // simulate slow back end store
            store.lock.lock();
            try {
               cache.remove("X");

               assert null == cache.get("X") : "cache must return X == null";
            } finally {
               store.lock.unlock();
            }
         }
      });
   }

   private Cache getCache() {
      return AbstractCacheStoreTest.mockCache(getClass().getName());
   }
}
