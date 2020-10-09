package org.infinispan.persistence.support;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.async.AsyncNonBlockingStore;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
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
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.support.AsyncStoreTest", singleThreaded = true)
public class AsyncStoreTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(AsyncStoreTest.class);
   private AsyncNonBlockingStore<Object, Object> store;
   private TestObjectStreamMarshaller marshaller;

   private InitializationContext createStore() throws PersistenceException {
      return createStore(false);
   }

   private InitializationContext createStore(boolean slow) throws PersistenceException {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      DummyInMemoryStoreConfigurationBuilder dummyCfg = builder
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(AsyncStoreTest.class.getName())
            .segmented(false);
      dummyCfg
         .async()
            .enable();
      dummyCfg.slow(slow);
      InitializationContext ctx = PersistenceMockUtil.createContext(getClass(), builder.build(), marshaller);
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

   @Test(timeOut=30000)
   public void testConcurrentClearAndStop() throws Exception {
      TestResourceTracker.testThreadStarted(this.getTestName());
      createStore(true);

      // start a thread that keeps clearing the store until its stopped
      Future<Void> f = fork(() -> {
         try {
            for (;;) {
               // All of our stores don't support calling stop at the same time as other operations
               // PersistenceManagerImpl normally ensure this, but this test does it directly
               synchronized (AsyncStoreTest.this) {
                  store.clear();
               }
            }
         } catch (CacheException expected) {
         }
         return null;
      });

      // wait until thread has started
      Thread.sleep(500);
      synchronized (this) {
         CompletionStages.join(store.stop());
      }

      // background thread should exit with CacheException
      f.get(1000, TimeUnit.SECONDS);
   }

   private void doTestPut(int number, String key, String value) {
      for (int i = 0; i < number; i++) {
         InternalCacheEntry cacheEntry = TestInternalCacheEntryFactory.create(key + i, value + i);
         store.write(0, MarshalledEntryUtil.create(cacheEntry, marshaller));
      }

      for (int i = 0; i < number; i++) {
         MarshallableEntry me = CompletionStages.join(store.load(0, key + i));
         assertNotNull(me);
         assertEquals(value + i, me.getValue());
      }
   }

   private void doTestSameKeyPut(int number, String key, String value) {
      for (int i = 0; i < number; i++) {
         store.write(0, MarshalledEntryUtil.create(key, value + i, marshaller));
      }
      MarshallableEntry me = CompletionStages.join(store.load(0, key));
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

   private final static ThreadLocal<DelayableStore> STORE = new ThreadLocal<>();

   @BuiltBy(LockableStoreConfigurationBuilder.class)
   @ConfigurationFor(DelayableStore.class)
   public static class DelayableStoreConfiguration extends DummyInMemoryStoreConfiguration {

      public DelayableStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
         super(attributes, async);
      }
   }

   public static class LockableStoreConfigurationBuilder extends DummyInMemoryStoreConfigurationBuilder {

      public LockableStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder);
      }

      @Override
      public DelayableStoreConfiguration create() {
         return new DelayableStoreConfiguration(attributes.protect(), async.create());
      }
   }

   public static class DelayableStore extends DummyInMemoryStore {
      private volatile CompletableFuture<Void> delayedFuture;

      public DelayableStore() {
         super();
         STORE.set(this);
      }

      @Override
      public CompletionStage<Void> write(int segment, MarshallableEntry entry) {
         CompletionStage<Void> actualStage = super.write(segment, entry);
         if (delayedFuture != null) {
            return actualStage.thenCompose(ignore -> delayedFuture);
         }
         return actualStage;
      }

      @Override
      public CompletionStage<Boolean> delete(int segment, Object key) {
         CompletionStage<Boolean> actualStage = super.delete(segment, key);
         if (delayedFuture != null) {
            return actualStage.thenCompose(removed -> delayedFuture
                  .thenCompose(ignore -> CompletableFutures.booleanStage(removed)));
         }
         return actualStage;
      }
   }

   public void testModificationQueueSize(final Method m) {
      int queueSize = 10;
      DelayableStore underlying = new DelayableStore();
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      LockableStoreConfigurationBuilder lcscsBuilder = (LockableStoreConfigurationBuilder) builder
            .persistence()
            .addStore(new LockableStoreConfigurationBuilder(builder.persistence()));
      lcscsBuilder.async()
            .modificationQueueSize(queueSize);

      store = new AsyncNonBlockingStore<>(underlying);
      InitializationContext ctx =
            PersistenceMockUtil.createContext(getClass(), builder.build(), marshaller);
      CompletionStages.join(store.start(ctx));
      // Delay all the underlying store completions until we complete this future
      underlying.delayedFuture = new CompletableFuture<>();
      try {
         Future<Void> f = fork(() -> {
            try {
               // The first will be in the process of replicating - thus we need one additional to fill the queue
               // so the one below will block on the join
               for (int i = 0; i < queueSize + 1; i++)
                  CompletionStages.join(store.write(0, MarshalledEntryUtil.create(k(m, i), v(m, i), marshaller)));
               // This one should block as we have exhausted the queue
               CompletionStages.join(store.write(0, MarshalledEntryUtil.create(k(m, queueSize + 1), v(m, queueSize + 1), marshaller)));
            } catch (Exception e) {
               log.error("Error storing entry", e);
            }
            return null;
         });
         Exceptions.expectException(TimeoutException.class, () -> f.get(1, TimeUnit.SECONDS));

         // Size currently doesn't look at the pending replication queue
         assertEquals(1, underlying.size());
         // Let the task finish
         underlying.delayedFuture.complete(null);
      } finally {
         CompletionStages.join(store.stop());
      }
   }

   private static abstract class OneEntryCacheManagerCallable extends CacheManagerCallable {
      protected final Cache<String, String> cache;
      protected final DelayableStore store;

      private static ConfigurationBuilder config(boolean passivation) {
         ConfigurationBuilder config = new ConfigurationBuilder();
         config.memory().maxCount(1).persistence().passivation(passivation).addStore(LockableStoreConfigurationBuilder.class)
               .async()
                  .modificationQueueSize(1)
                  .enable();
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
         public void call() throws InterruptedException {
            cache.put("X", "1");
            cache.put("Y", "1"); // force eviction of "X"

            // wait for X to appear in store
            eventually(() -> store.loadEntry("X") != null);

            // simulate slow back end store
            store.delayedFuture = new CompletableFuture<>();
            try {
               cache.put("X", "2");
               // Needs to be in other thread as non blocking store is invoked in same thread
               Future<Void> f = fork(() -> {
                  cache.put("Y", "2"); // force eviction of "X"
                  if (!passivation) {
                     cache.put("Z", "1"); // force eviction of "Y"
                  }
               });

               Exceptions.expectException(TimeoutException.class, () -> f.get(100, TimeUnit.MILLISECONDS));

               assertEquals("2", cache.get("X"));
               if (!passivation) {
                  assertEquals("1", cache.get("Z"));
               }
            } finally {
               store.delayedFuture.complete(null);
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
            store.delayedFuture = new CompletableFuture<>();
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
               store.delayedFuture.complete(null);
            }
         }
      });
   }
}
