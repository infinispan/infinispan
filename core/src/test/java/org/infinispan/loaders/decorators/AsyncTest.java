package org.infinispan.loaders.decorators;

import org.infinispan.CacheException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Prepare;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransactionFactory;
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

import static org.infinispan.test.TestingUtil.*;

@Test(groups = "unit", testName = "loaders.decorators.AsyncTest")
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
      asyncConfig = new AsyncStoreConfig();
      asyncConfig.setThreadPoolSize(10);
      store = new AsyncStore(underlying, asyncConfig);
      dummyCfg = new DummyInMemoryCacheStore.Cfg();
      dummyCfg.setStore(AsyncTest.class.getName());
      store.init(dummyCfg, null, null);
      store.start();
      asyncExecutor = (ExecutorService) TestingUtil.extractField(store, "executor");
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      if (store != null) store.stop();
   }

   public void testPutRemove() throws Exception {
      final int number = 1000;
      String key = "testPutRemove-k-";
      String value = "testPutRemove-v-";
      doTestPut(number, key, value);
      doTestRemove(number, key);
   }

   public void testPutClearPut() throws Exception {
      final int number = 1000;
      String key = "testPutClearPut-k-";
      String value = "testPutClearPut-v-";
      doTestPut(number, key, value);
      doTestClear(number, key);
      value = "testPutClearPut-v[2]-";
      doTestPut(number, key, value);

      doTestRemove(number, key);
   }

   public void testMultiplePutsOnSameKey() throws Exception {
      final int number = 1000;
      String key = "testMultiplePutsOnSameKey-k";
      String value = "testMultiplePutsOnSameKey-v-";
      doTestSameKeyPut(number, key, value);
      doTestSameKeyRemove(key);
   }

   public void testRestrictionOnAddingToAsyncQueue() throws Exception {
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
         dummyCfg.setStore(m.getName());
         store.init(dummyCfg, null, null);
         store.start();

         store.store(InternalEntryFactory.create(key, "v1"));
         v2Latch.await();
         store.store(InternalEntryFactory.create(key, "v2"));
         endLatch.await();

         assert store.load(key).getValue().equals("v2");
      } finally {
         store.delegate.clear();
         store.stop();
         store = null;
      }
   }

   public void testTransactionalModifications(Method m) throws Exception {
      try {
         final GlobalTransactionFactory gtf = new GlobalTransactionFactory();
         final String k1 = k(m, "1"), k2 = k(m, "2"), v1 = v(m, "1"), v2 = v(m, "2");
         final ConcurrentMap<Object, Modification> localMods = new ConcurrentHashMap<Object, Modification>();
         final CyclicBarrier barrier = new CyclicBarrier(2);
         DummyInMemoryCacheStore underlying = new DummyInMemoryCacheStore();
         store = new AsyncStore(underlying, asyncConfig) {
            @Override
            protected void applyModificationsSync(ConcurrentMap<Object, Modification> mods) throws CacheLoaderException {
               for (Map.Entry<Object, Modification> entry : mods.entrySet()) {
                  localMods.put(entry.getKey(), entry.getValue());
               }
//               try {
//                  barrier.await(5, TimeUnit.SECONDS);
//               } catch (TimeoutException e) {
//                  assert false : "Timed out waiting for modifications";
//               } catch (Exception e) {
//                  throw new CacheLoaderException("Barried failed", e);
//               }
               super.applyModificationsSync(mods);
               try {
                  barrier.await(5, TimeUnit.SECONDS);
               } catch (TimeoutException e) {
                  assert false : "Timed out applying for modifications";
               } catch (Exception e) {
                  throw new CacheLoaderException("Barried failed", e);
               }
            }
         };
         dummyCfg = new DummyInMemoryCacheStore.Cfg();
         dummyCfg.setStore(m.getName());
         store.init(dummyCfg, null, null);
         store.start();

         List<Modification> mods = new ArrayList<Modification>();
         mods.add(new Store(InternalEntryFactory.create(k1, v1)));
         mods.add(new Store(InternalEntryFactory.create(k2, v2)));
         mods.add(new Remove(k1));
         GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
         store.prepare(mods, tx, false);
         barrier.await(5, TimeUnit.SECONDS);
//         barrier.await(5, TimeUnit.SECONDS);

         assert 1 == localMods.size();
         assert localMods.entrySet().iterator().next().getKey() instanceof Prepare;
         assert !store.containsKey(k1);
         assert !store.containsKey(k2);

         store.commit(tx);
         barrier.await(5, TimeUnit.SECONDS);
//         barrier.await(5, TimeUnit.SECONDS);
         assert store.load(k2).getValue().equals(v2);
         assert !store.containsKey(k1);
      } finally {
         store.delegate.clear();
         store.stop();
         store = null;
      }
   }

   private void doTestPut(int number, String key, String value) throws Exception {
      for (int i = 0; i < number; i++) store.store(InternalEntryFactory.create(key + i, value + i));

      TestingUtil.sleepRandom(1000);

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
                  TestingUtil.sleepRandom(1000);
               }
            }
         }
      }
   }

   private void doTestSameKeyPut(int number, String key, String value) throws Exception {
      for (int i = 0; i < number; i++)
         store.store(InternalEntryFactory.create(key, value + i));

      TestingUtil.sleepThread(5000);
      InternalCacheEntry entry;
      boolean success = false;
      for (int i = 0; i < 120; i++) {
         TestingUtil.sleepRandom(1000);
         entry = store.load(key);
         success = entry.getValue().equals(value + (number - 1));
         if (success) break;
      }
      assert success;
   }

   private void doTestRemove(int number, String key) throws Exception {
      for (int i = 0; i < number; i++) store.remove(key + i);

      TestingUtil.sleepRandom(1000);

      InternalCacheEntry[] entries = new InternalCacheEntry[number];
      for (int i = 0; i < number; i++) {
         entries[i] = store.load(key + i);
      }

      for (int i = 0; i < number; i++) {
         InternalCacheEntry entry = entries[i];
         while (entry != null) {
            log.info("Entry still not null {0}", entry);
            TestingUtil.sleepRandom(1000);
            entry = store.load(key + i);
         }
      }
   }

   private void doTestSameKeyRemove(String key) throws Exception {
      store.remove(key);
      InternalCacheEntry entry;
      do {
         TestingUtil.sleepRandom(1000);
         entry = store.load(key);
      } while (entry != null);
   }

   private void doTestClear(int number, String key) throws Exception {
      store.clear();
      TestingUtil.sleepRandom(1000);

      InternalCacheEntry[] entries = new InternalCacheEntry[number];
      for (int i = 0; i < number; i++) {
         entries[i] = store.load(key + i);
      }

      for (int i = 0; i < number; i++) {
         InternalCacheEntry entry = entries[i];
         while (entry != null) {
            log.info("Entry still not null {0}", entry);
            TestingUtil.sleepRandom(1000);
            entry = store.load(key + i);
         }
      }
   }

   class MockAsyncStore extends AsyncStore {
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
            if (log.isTraceEnabled()) log.trace("Wait for v1 latch");
            try {
               v2Latch.countDown();
               block = false;
               v1Latch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            super.applyModificationsSync(mods);
         } else if (mods.get(key) != null && !block) {
            if (log.isTraceEnabled()) log.trace("Do v2 modification and unleash v1 latch");
            super.applyModificationsSync(mods);
            v1Latch.countDown();
            endLatch.countDown();
         }
      }

   }

   ;
}
