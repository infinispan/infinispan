package org.infinispan.loaders.decorators;

import org.infinispan.CacheException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;

@Test(groups = "unit", testName = "loaders.decorators.AsyncTest")
public class AsyncTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(AsyncTest.class);
   AsyncStore store;
   ExecutorService asyncExecutor;
   DummyInMemoryCacheStore underlying;
   AsyncStoreConfig asyncConfig;
   DummyInMemoryCacheStore.Cfg dummyCfg;

   @BeforeTest
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

   @AfterTest
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

      InternalCacheEntry entry;
      boolean success = false;
      for (int i = 0; i < 120; i++) {
         TestingUtil.sleepRandom(1000);
         entry = store.load(key);
         success = entry.getValue().equals(value + (number-1));
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

}
