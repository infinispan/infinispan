package org.infinispan.persistence.support;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.async.AsyncNonBlockingStore;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.support.AsyncStoreTest")
public class AsyncStoreTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(AsyncStoreTest.class);
   private AsyncNonBlockingStore<Object, Object> store;
   private TestObjectStreamMarshaller marshaller;

   private final int CACHE_SEGMENT_MAX = 256;

   private InitializationContext createStore() throws PersistenceException {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      DummyInMemoryStoreConfigurationBuilder dummyCfg = builder
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  // Async store doesn't help much if operations are always synchronous
                  .asyncOperation(true)
                  .storeName(AsyncStoreTest.class.getName())
            .segmented(false);
      dummyCfg
         .async()
            .enable();
      InitializationContext testCtx = PersistenceMockUtil.createContext(getClass(), builder.build(), marshaller);
      InitializationContext ctx = new DelegatingInitializationContext() {
         @Override
         public InitializationContext delegate() {
            return testCtx;
         }

         @Override
         public Executor getNonBlockingExecutor() {
            // The testCtx gives a WithinExecutor
            return testExecutor();
         }
      };
      DummyInMemoryStore underlying = new DummyInMemoryStore();
      store = new AsyncNonBlockingStore(underlying);
      CompletionStages.join(store.start(ctx));

      return ctx;
   }

   @BeforeMethod
   public void createMarshalledEntryFactory() {
      marshaller = new TestObjectStreamMarshaller();
   }

   @AfterMethod
   public void tearDown() throws PersistenceException {
      if (store != null) CompletionStages.join(store.stop());
      marshaller.stop();
   }

   @Test(timeOut=30000)
   public void testPutRemove() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      createStore();

      final int number = 1000;
      String key = "testPutRemove-k-";
      String value = "testPutRemove-v-";
      doTestPut(number, key, value);
      doTestRemove(number, key);
   }

   @Test(timeOut=30000)
   public void testRepeatedPutRemove() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      createStore();

      final int number = 10;
      final int loops = 2000;
      String key = "testRepeatedPutRemove-k-";
      String value = "testRepeatedPutRemove-v-";

      int failures = 0;
      for (int i = 0; i < loops; i++) {
         try {
            doTestPut(number, key, value);
            doTestRemove(number, key);
         } catch (Error e) {
            failures++;
         }
      }
      assertEquals(0, failures);
   }

   @Test(timeOut=30000)
   public void testPutClearPut() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
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

   @Test(timeOut=30000)
   public void testRepeatedPutClearPut() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      createStore();

      final int number = 10;
      final int loops = 2000;
      String key = "testRepeatedPutClearPut-k-";
      String value = "testRepeatedPutClearPut-v-";
      String value2 = "testRepeatedPutClearPut-v[2]-";

      int failures = 0;
      for (int i = 0; i < loops; i++) {
         try {
            doTestPut(number, key, value);
            doTestClear(number, key);
            doTestPut(number, key, value2);
         } catch (Error e) {
            failures++;
         }
      }
      assertEquals(0, failures);
   }

   @Test(timeOut=30000)
   public void testMultiplePutsOnSameKey() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      createStore();

      final int number = 1000;
      String key = "testMultiplePutsOnSameKey-k";
      String value = "testMultiplePutsOnSameKey-v-";
      doTestSameKeyPut(number, key, value);
      doTestSameKeyRemove(key);
   }

   @Test(timeOut=30000)
   public void testRestrictionOnAddingToAsyncQueue() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      InitializationContext ctx = createStore();

      store.delete(0, "blah");

      final int number = 10;
      String key = "testRestrictionOnAddingToAsyncQueue-k";
      String value = "testRestrictionOnAddingToAsyncQueue-v-";
      doTestPut(number, key, value);

      // stop the cache store
      CompletionStages.join(store.stop());
      try {
         store.write(0, MarshalledEntryUtil.create("k", marshaller));
         fail("Should have restricted this entry from being made");
      }
      catch (CacheException expected) {
      }

      // clean up
      CompletionStages.join(store.start(ctx));
      doTestRemove(number, key);
   }

   public int segmentForKey(Object key) {
      return String.valueOf(key).hashCode() % CACHE_SEGMENT_MAX;
   }

   private void doTestPut(int number, String key, String value) {
      for (int i = 0; i < number; i++) {
         InternalCacheEntry cacheEntry = TestInternalCacheEntryFactory.create(key + i, value + i);
         store.write(segmentForKey(cacheEntry.getKey()), MarshalledEntryUtil.create(cacheEntry, marshaller));
      }

      for (int i = 0; i < number; i++) {
         String keyStr = key + i;
         MarshallableEntry me = CompletionStages.join(store.load(segmentForKey(keyStr), key + i));
         assertNotNull(me);
         assertEquals(value + i, me.getValue());
      }
   }

   private void doTestSameKeyPut(int number, String key, String value) {
      int segment = segmentForKey(key);
      for (int i = 0; i < number; i++) {
         store.write(segment, MarshalledEntryUtil.create(key, value + i, marshaller));
      }
      MarshallableEntry me = CompletionStages.join(store.load(segment, key));
      assertNotNull(me);
      assertEquals(value + (number - 1), me.getValue());
   }

   private void doTestRemove(final int number, final String key) throws Exception {
      for (int i = 0; i < number; i++) store.delete(0, key + i);
      for (int i = 0; i < number; i++) assertNull(CompletionStages.join(store.load(0, key + i)));
   }

   private void doTestSameKeyRemove(String key) {
      store.delete(0, key);
      assertNull(CompletionStages.join(store.load(0, key)));
   }

   private void doTestClear(int number, String key) throws Exception {
      CompletionStages.join(store.clear());

      for (int i = 0; i < number; i++) {
         assertNull(CompletionStages.join(store.load(0, key + i)));
      }
   }

   public void testModificationQueueSize(final Method m) throws Exception {
      int queueSize = 5;
      DelayStore underlying = new DelayStore();
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      builder.persistence()
             .addStore(DelayStore.ConfigurationBuilder.class)
             .async()
             .modificationQueueSize(queueSize);

      store = new AsyncNonBlockingStore<>(underlying);
      InitializationContext ctx = PersistenceMockUtil.createContext(getClass(), builder.build(), marshaller);
      CompletionStages.join(store.start(ctx));
      // Delay all the underlying store completions until we complete this future
      underlying.delayAfterModification(queueSize + 2);
      try {
         CountDownLatch queueFullLatch = new CountDownLatch(1);
         Future<Void> f = fork(() -> {
            // Fill the modifications queue
            for (int i = 0; i < queueSize; i++)
               CompletionStages.join(store.write(0, MarshalledEntryUtil.create(k(m, i), v(m, i), marshaller)));

            // The next one should block
            // The first modification is already replicating, but the store still counts it against the queue size
            CompletionStage<Void> blockedWrite =
                  store.write(0, MarshalledEntryUtil.create(k(m, queueSize + 1), v(m, queueSize + 1), marshaller));
            assertFalse(blockedWrite.toCompletableFuture().isDone());

            queueFullLatch.countDown();
            CompletionStages.join(blockedWrite);
         });

         assertTrue(queueFullLatch.await(10, TimeUnit.SECONDS));

         Thread.sleep(50);
         assertFalse(f.isDone());

         // Size currently doesn't look at the pending replication queue
         assertEquals(1, underlying.size());
         assertEquals(1, (long) CompletionStages.join(store.size(IntSets.immutableSet(0))));

         // Let the task finish
         underlying.endDelay();

         f.get(10, TimeUnit.SECONDS);
         assertEquals(queueSize + 1, underlying.size());
      } finally {
         underlying.endDelay();
         CompletionStages.join(store.stop());
      }
   }

   private abstract static class OneEntryCacheManagerCallable extends CacheManagerCallable {
      protected final Cache<String, String> cache;
      protected final DelayStore store;

      private static ConfigurationBuilder config(boolean passivation) {
         ConfigurationBuilder config = new ConfigurationBuilder();
         config.memory().maxCount(1)
               .persistence().passivation(passivation)
               .addStore(DelayStore.ConfigurationBuilder.class)
               .async()
                  // This cannot be 1. When using passivation in doTestEndToEndPutPut we block a store write to key X.
                  // This would then mean we have a batch of 1 we would not allow any subsequent writes to complete
                  // as they will be enqueued. This allows us to let the test block passivation but continue.
                  .modificationQueueSize(2)
                  .enable();
         return config;
      }

      OneEntryCacheManagerCallable(boolean passivation) {
         super(TestCacheManagerFactory.createCacheManager(config(passivation)));
         cache = cm.getCache();
         store = TestingUtil.getFirstStore(cache);
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
         public void call() throws InterruptedException {
            cache.put("X", "1");
            cache.put("Y", "1"); // force eviction of "X"

            // wait for X to appear in store
            eventually(() -> store.loadEntry("X") != null);

            // simulate slow back end store
            store.delayAfterModification(3);
            try {
               cache.put("X", "2");
               // Needs to be in other thread as non blocking store is invoked in same thread
               CompletionStage<String> stage = cache.putAsync("Y", "2"); // force eviction of "X"
               if (!passivation) {
                  CompletionStages.join(stage);
                  cache.putAsync("Z", "1"); // force eviction of "Y"
               }

               assertEquals("2", cache.get("X"));
               if (!passivation) {
                  assertEquals("1", cache.get("Z"));
               }
            } finally {
               store.endDelay();
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
         public void call() throws InterruptedException {
            cache.put("X", "1");
            cache.put("Y", "1"); // force eviction of "X"

            // wait for X to appear in store
            eventually(() -> store.loadEntry("X") != null);

            // simulate slow back end store
            store.delayAfterModification(3);
            try {
               cache.put("replicating", "completes, but replication is stuck on delayed Future");
               if (!passivation) {
                  cache.put("in-queue", "completes, but waiting on previous replication to complete before replicating");
               }
               // Needs to be in other thread as non blocking store is invoked in same thread
               Future<Void> f = fork(() -> {
                  // This will not return since the replication queue is full from in-queue
                  cache.remove("X");
               });

               Exceptions.expectException(TimeoutException.class, () -> f.get(100, TimeUnit.MILLISECONDS));
            } finally {
               store.endDelay();
            }
         }
      });
   }
}
