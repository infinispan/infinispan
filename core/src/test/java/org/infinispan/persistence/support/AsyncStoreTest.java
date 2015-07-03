package org.infinispan.persistence.support;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.async.AdvancedAsyncCacheLoader;
import org.infinispan.persistence.async.AdvancedAsyncCacheWriter;
import org.infinispan.persistence.async.State;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.Remove;
import org.infinispan.persistence.modifications.Store;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.PersistenceMockUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.marshalledEntry;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.*;

@Test(groups = "unit", testName = "persistence.support.AsyncStoreTest", sequential=true)
public class AsyncStoreTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(AsyncStoreTest.class);
   private AdvancedAsyncCacheWriter writer;
   private AdvancedAsyncCacheLoader loader;
   private TestObjectStreamMarshaller marshaller;

   private void createStore() throws PersistenceException {
      createStore(false);
   }

   /**
    * Creates State objects with slightly throttled get() performance, to check that load() is
    * really in sync with changes. This gives the coordinator thread a better chance to execute
    * while AsyncCacheLoader.load() is iterating states.
    */
   static class SlowAdvancedAsyncCacheWriter extends AdvancedAsyncCacheWriter {
      public SlowAdvancedAsyncCacheWriter(CacheWriter delegate) {
         super(delegate);
      }
      @Override
      protected State newState(boolean clear, State next) {
         ConcurrentMap<Object, Modification> map = new ConcurrentHashMap() {
            @Override
            public Object get(Object key) {
               Object result = super.get(key);
               for (int i = 0; i < 10; i++)
                  Thread.yield();
               return result;
            }
         };
         return new State(clear, map, next);
      }
   };

   private void createStore(boolean slow) throws PersistenceException {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      DummyInMemoryStoreConfigurationBuilder dummyCfg = builder
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(AsyncStoreTest.class.getName());
      dummyCfg
         .async()
            .enable()
            .threadPoolSize(10);
      dummyCfg.slow(slow);
      DummyInMemoryStore underlying = new DummyInMemoryStore();
      writer = new SlowAdvancedAsyncCacheWriter(underlying);
      InitializationContext ctx = PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller);
      writer.init(ctx);
      writer.start();
      loader = new AdvancedAsyncCacheLoader(underlying, writer.getState());
      loader.init(ctx);
      loader.start();
      underlying.init(ctx);
      underlying.start();
   }

   @BeforeMethod
   public void createMarshaller() {
      marshaller = new TestObjectStreamMarshaller();
   }

   @AfterMethod
   public void tearDown() throws PersistenceException {
      if (writer != null) writer.stop();
      if (loader != null) loader.stop();
      marshaller.stop();
   }

   @Test(timeOut=30000)
   public void testPutRemove() throws Exception {
      TestResourceTracker.testThreadStarted(this);
      createStore();

      final int number = 1000;
      String key = "testPutRemove-k-";
      String value = "testPutRemove-v-";
      doTestPut(number, key, value);
      doTestRemove(number, key);
   }

   @Test(timeOut=30000)
   public void testRepeatedPutRemove() throws Exception {
      TestResourceTracker.testThreadStarted(this);
      createStore();

      final int number = 10;
      final int loops = 5000;
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
      TestResourceTracker.testThreadStarted(this);
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
      TestResourceTracker.testThreadStarted(this);
      createStore();

      final int number = 10;
      final int loops = 5000;
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
      TestResourceTracker.testThreadStarted(this);
      createStore();

      final int number = 1000;
      String key = "testMultiplePutsOnSameKey-k";
      String value = "testMultiplePutsOnSameKey-v-";
      doTestSameKeyPut(number, key, value);
      doTestSameKeyRemove(key);
   }

   @Test(timeOut=30000)
   public void testRestrictionOnAddingToAsyncQueue() throws Exception {
      TestResourceTracker.testThreadStarted(this);
      createStore();

      writer.delete("blah");

      final int number = 10;
      String key = "testRestrictionOnAddingToAsyncQueue-k";
      String value = "testRestrictionOnAddingToAsyncQueue-v-";
      doTestPut(number, key, value);

      // stop the cache store
      writer.stop();
      try {
         writer.write(new MarshalledEntryImpl("k", (Object) null, null, marshaller()));
         fail("Should have restricted this entry from being made");
      }
      catch (CacheException expected) {
      }

      // clean up
      writer.start();
      doTestRemove(number, key);
   }

   private TestObjectStreamMarshaller marshaller() {
      return marshaller;
   }

   public void testThreadSafetyWritingDiffValuesForKey(Method m) throws Exception {
      try {
         final String key = "k1";
         final CountDownLatch v1Latch = new CountDownLatch(1);
         final CountDownLatch v2Latch = new CountDownLatch(1);
         final CountDownLatch endLatch = new CountDownLatch(1);
         DummyInMemoryStore underlying = new DummyInMemoryStore();
         writer = new MockAsyncCacheWriter(key, v1Latch, v2Latch, endLatch, underlying);
         ConfigurationBuilder builder = TestCacheManagerFactory
               .getDefaultCacheConfiguration(false);
         builder
               .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(m.getName());
         Configuration configuration = builder.build();
         InitializationContext ctx = PersistenceMockUtil.createContext(getClass().getSimpleName(), configuration, marshaller);
         writer.init(ctx);
         writer.start();
         underlying.init(ctx);
         underlying.start();

         writer.write(new MarshalledEntryImpl(key, "v1", null, marshaller()));
         v2Latch.await();
         writer.write(new MarshalledEntryImpl(key, "v2", null, marshaller()));
         if (!endLatch.await(30000l, TimeUnit.MILLISECONDS))
            fail();

         loader = new AdvancedAsyncCacheLoader(underlying, writer.getState());
         assertEquals("v2", loader.load(key).getValue());
      } finally {
         writer.clear();
         writer.stop();
         writer = null;
      }
   }

   @Test(timeOut=30000)
   public void testConcurrentWriteAndStop() throws Exception {
      TestResourceTracker.testThreadStarted(this);
      createStore(true);

      final int lastValue[] = { 0 };
      // start a thread that keeps writing new values for the same key, until the store is stopped
      final String key = "testConcurrentWriteAndStop";
      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               for (;;) {
                  int v = lastValue[0] + 1;
                  writer.write(new MarshalledEntryImpl(key, key + v, null, marshaller()));
                  lastValue[0] = v;
               }
            } catch (CacheException expected) {
            }
         }
      };
      t.start();

      // wait until thread has written some values
      Thread.sleep(500);
      writer.stop();

      // check that the last value successfully written to the AsyncStore has also been written to the underlying store
      MarshalledEntry me = loader.undelegate().load(key);
      assertNotNull(me);
      assertEquals(me.getValue(), key + lastValue[0]);
   }

   @Test(timeOut=30000)
   public void testConcurrentClearAndStop() throws Exception {
      TestResourceTracker.testThreadStarted(this);
      createStore(true);

      // start a thread that keeps clearing the store until its stopped
      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               for (;;)
                  writer.clear();
            } catch (CacheException expected) {
            }
         }
      };
      t.start();

      // wait until thread has started
      Thread.sleep(500);
      writer.stop();

      // background thread should exit with CacheException
      t.join(1000);
      assertFalse(t.isAlive());
   }

   private void doTestPut(int number, String key, String value) throws Exception {
      for (int i = 0; i < number; i++) {
         InternalCacheEntry cacheEntry = TestInternalCacheEntryFactory.create(key + i, value + i);
         writer.write(marshalledEntry(cacheEntry, marshaller()));
      }

      for (int i = 0; i < number; i++) {
         MarshalledEntry me = loader.load(key + i);
         assertNotNull(me);
         assertEquals(value + i, me.getValue());
      }
   }

   private void doTestSameKeyPut(int number, String key, String value) throws Exception {
      for (int i = 0; i < number; i++) {
         writer.write(new MarshalledEntryImpl(key, value + i, null, marshaller()));
      }
      MarshalledEntry me = loader.load(key);
      assertNotNull(me);
      assertEquals(value + (number - 1), me.getValue());
   }

   private void doTestRemove(final int number, final String key) throws Exception {
      for (int i = 0; i < number; i++) writer.delete(key + i);
      for (int i = 0; i < number; i++) assertNull(loader.load(key + i));
   }

   private void doTestSameKeyRemove(String key) throws Exception {
      writer.delete(key);
      assertNull(loader.load(key));
   }

   private void doTestClear(int number, String key) throws Exception {
      writer.clear();

      for (int i = 0; i < number; i++) {
         assertNull(loader.load(key + i));
      }
   }

   static class MockAsyncCacheWriter extends AdvancedAsyncCacheWriter {
      volatile boolean block = true;
      final CountDownLatch v1Latch;
      final CountDownLatch v2Latch;
      final CountDownLatch endLatch;
      final Object key;

      MockAsyncCacheWriter(Object key, CountDownLatch v1Latch, CountDownLatch v2Latch, CountDownLatch endLatch,
                           CacheWriter delegate) {
         super(delegate);
         this.v1Latch = v1Latch;
         this.v2Latch = v2Latch;
         this.endLatch = endLatch;
         this.key = key;
      }

      @Override
      protected void applyModificationsSync(List<Modification> mods) throws PersistenceException {
         boolean keyFound = findModificationForKey(key, mods) != null;
         if (keyFound && block) {
            log.trace("Wait for v1 latch" + mods);
            try {
               v2Latch.countDown();
               block = false;
               log.trace("before wait");
               v1Latch.await(2, TimeUnit.SECONDS);
               log.trace("after wait");
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
            log.trace("before apply mods");
            try {
               super.applyModificationsSync(mods);
            } catch (Throwable e) {
               log.trace("Error apply mods :" + e.getMessage());
            }
            log.trace("after apply mods");
         } else if (keyFound && !block) {
            log.trace("Do v2 modification and unleash v1 latch" + mods);
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
                  if (store.getKey().equals(key))
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

   private final static ThreadLocal<LockableStore> STORE = new ThreadLocal<LockableStore>();

   @BuiltBy(LockableStoreConfigurationBuilder.class)
   @ConfigurationFor(LockableStore.class)
   public static class LockableStoreConfiguration extends DummyInMemoryStoreConfiguration {

      public LockableStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
         super(attributes, async, singletonStore);
      }
   }

   public static class LockableStoreConfigurationBuilder extends DummyInMemoryStoreConfigurationBuilder {

      public LockableStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder);
      }

      @Override
      public LockableStoreConfiguration create() {
         return new LockableStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
      }
   }

   public static class LockableStore extends DummyInMemoryStore {
      private final ReentrantLock lock = new ReentrantLock();
      private final Set<Thread> threads = new HashSet<>();

      public LockableStore() {
         super();
         STORE.set(this);
      }

      @Override
      public void write(MarshalledEntry entry) {
         lock.lock();
         try {
            threads.add(Thread.currentThread());
            super.write(entry);
         } finally {
            lock.unlock();
         }
      }

      @Override
      public boolean delete(Object key) {
         lock.lock();
         try {
            threads.add(Thread.currentThread());
            return super.delete(key);
         } finally {
            lock.unlock();
         }
      }
   }

   public void testModificationQueueSize(final Method m) throws Exception {
      LockableStore underlying = new LockableStore();
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      LockableStoreConfigurationBuilder lcscsBuilder = (LockableStoreConfigurationBuilder) builder
            .persistence()
            .addStore(new LockableStoreConfigurationBuilder(builder.persistence()));
      lcscsBuilder.async()
            .modificationQueueSize(10)
            .threadPoolSize(3);
      lcscsBuilder.async()
            .shutdownTimeout(50);

      writer = new AdvancedAsyncCacheWriter(underlying);
      writer.init(PersistenceMockUtil.createContext(getClass().getSimpleName(), builder.build(), marshaller));
      writer.start();
      try {
         final CountDownLatch done = new CountDownLatch(1);

         underlying.lock.lock();
         try {
            Thread t = new Thread() {
               @Override
               public void run() {
                  try {
                     for (int i = 0; i < 100; i++)
                        writer.write(new MarshalledEntryImpl(k(m, i), v(m, i), null, marshaller()));
                  } catch (Exception e) {
                     log.error("Error storing entry", e);
                  }
                  done.countDown();
               }
            };
            t.start();

            assertFalse("Background thread should have blocked after adding 10 entries", done.await(1, TimeUnit.SECONDS));
         } finally {
            underlying.lock.unlock();
         }
      } finally {
         writer.stop();
      }
      assertEquals(3, underlying.threads.size());
   }

   private static abstract class OneEntryCacheManagerCallable extends CacheManagerCallable {
      protected final Cache<String, String> cache;
      protected final LockableStore store;

      private static ConfigurationBuilder config(boolean passivation) {
         ConfigurationBuilder config = new ConfigurationBuilder();
         config.eviction().maxEntries(1).persistence().passivation(passivation).addStore(LockableStoreConfigurationBuilder.class).async().enable();
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

               assertEquals("cache must return X == 2", "2", cache.get("X"));
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

               assertNull(cache.get("X"));
            } finally {
               store.lock.unlock();
            }
         }
      });
   }
}
