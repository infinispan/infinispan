package org.infinispan.persistence.support;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.async.AdvancedAsyncCacheLoader;
import org.infinispan.persistence.async.AdvancedAsyncCacheWriter;
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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.marshalledEntry;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.fail;

@Test(groups = "unit", testName = "persistence.support.AsyncStoreTest", sequential=true)
public class AsyncStoreTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(AsyncStoreTest.class);
   private AdvancedAsyncCacheWriter writer;
   private AdvancedAsyncCacheLoader loader;
   private TestObjectStreamMarshaller marshaller;

   private void createStore() throws PersistenceException {
      DummyInMemoryStoreConfigurationBuilder dummyCfg = TestCacheManagerFactory.getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(AsyncStoreTest.class.getName());
      dummyCfg
         .async()
            .enable()
            .threadPoolSize(10);
      DummyInMemoryStore underlying = new DummyInMemoryStore();
      writer = new AdvancedAsyncCacheWriter(underlying);
      DummyInitializationContext ctx = new DummyInitializationContext(dummyCfg.create(), getCache(), marshaller(), new ByteBufferFactoryImpl(), new MarshalledEntryFactoryImpl(marshaller()));
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
      TestCacheManagerFactory.backgroundTestStarted(this);
      createStore();

      final int number = 1000;
      String key = "testPutRemove-k-";
      String value = "testPutRemove-v-";
      doTestPut(number, key, value);
      doTestRemove(number, key);
   }

   @Test(timeOut=30000)
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

   @Test(timeOut=30000)
   public void testMultiplePutsOnSameKey() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
      createStore();

      final int number = 1000;
      String key = "testMultiplePutsOnSameKey-k";
      String value = "testMultiplePutsOnSameKey-v-";
      doTestSameKeyPut(number, key, value);
      doTestSameKeyRemove(key);
   }

   @Test(timeOut=30000)
   public void testRestrictionOnAddingToAsyncQueue() throws Exception {
      TestCacheManagerFactory.backgroundTestStarted(this);
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
         assert false : "Should have restricted this entry from being made";
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
         DummyInMemoryStoreConfigurationBuilder dummyCfg = TestCacheManagerFactory
               .getDefaultCacheConfiguration(false)
               .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(m.getName());
         DummyInitializationContext ctx = new DummyInitializationContext(dummyCfg.create(), getCache(), marshaller(),
                                                                         new ByteBufferFactoryImpl(),
                                                                         new MarshalledEntryFactoryImpl(marshaller()));
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
         assert loader.load(key).getValue().equals("v2");
      } finally {
         writer.clear();
         writer.stop();
         writer = null;
      }
   }

   private void doTestPut(int number, String key, String value) throws Exception {

      log.tracef("before write");

      for (int i = 0; i < number; i++) {
         InternalCacheEntry cacheEntry = TestInternalCacheEntryFactory.create(key + i, value + i);
         writer.write(marshalledEntry(cacheEntry, marshaller()));
      }

      log.tracef("after write");

      for (int i = 0; i < number; i++) {
         MarshalledEntry me = loader.load(key + i);
         assert me != null && (value + i).equals(me.getValue());
      }
   }

   private void doTestSameKeyPut(int number, String key, String value) throws Exception {
      for (int i = 0; i < number; i++) {
         writer.write(new MarshalledEntryImpl(key, value + i, null, marshaller()));
      }
      MarshalledEntry me = loader.load(key);
      assert me != null && (value + (number - 1)).equals(me.getValue());
   }

   private void doTestRemove(final int number, final String key) throws Exception {
      for (int i = 0; i < number; i++) writer.delete(key + i);

      eventually( new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            boolean allRemoved = true;

            for (int i = 0; i < number; i++) {
               String loadKey = key + i;
               if(loader.load(loadKey) != null) {
                  allRemoved = false;
                  break;
               }
            }

            return allRemoved;
         }
      });
   }

   private void doTestSameKeyRemove(String key) throws Exception {
      writer.delete(key);
      assert loader.load(key) == null;
   }

   private void doTestClear(int number, String key) throws Exception {
      log.trace("before clear");

      writer.clear();

      Thread.sleep(1000);
      log.trace("after clear");

      for (int i = 0; i < number; i++) {
         assert loader.load(key + i) == null;
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
            log("Wait for v1 latch" + mods);
            try {
               v2Latch.countDown();
               block = false;
               log("before wait");
               v1Latch.await(2, TimeUnit.SECONDS);
               log("after wait");
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
            log("before apply mods");
            try {
               super.applyModificationsSync(mods);
            } catch (Throwable e) {
               log("Error apply mods :" + e.getMessage());
            }
            log("after apply mods");
         } else if (keyFound && !block) {
            log("Do v2 modification and unleash v1 latch" + mods);
            super.applyModificationsSync(mods);
            v1Latch.countDown();
            endLatch.countDown();
         }
      }

      public void log(String m) {
//         System.out.println("[ " + Thread.currentThread() + " ] " + m );
         log.trace(m);
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

      public LockableStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties, boolean debug, boolean slow, String storeName, Object failKey) {
         super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties, debug, slow, storeName, failKey);
      }
   }

   public static class LockableStoreConfigurationBuilder extends DummyInMemoryStoreConfigurationBuilder {

      public LockableStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder);
      }

      @Override
      public LockableStoreConfiguration create() {
         return new LockableStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                               singletonStore.create(), preload, shared, properties, debug, slow, storeName, failKey);
      }
   }

   public static class LockableStore extends DummyInMemoryStore {
      private final ReentrantLock lock = new ReentrantLock();

      public LockableStore() {
         super();
         STORE.set(this);
      }

      @Override
      public void write(MarshalledEntry entry) {
         lock.lock();
         try {
            super.write(entry);
         } finally {
            lock.unlock();
         }

      }

      @Override
      public boolean delete(Object key) {
         lock.lock();
         try {
            return super.delete(key);
         } finally {
            lock.unlock();
         }
      }
   }

   public void testModificationQueueSize(final Method m) throws Exception {
      LockableStore underlying = new LockableStore();
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      LockableStoreConfigurationBuilder lcscsBuilder = new LockableStoreConfigurationBuilder(builder.persistence());
      lcscsBuilder.async()
            .modificationQueueSize(10);

      writer = new AdvancedAsyncCacheWriter(underlying);
      writer.init(new DummyInitializationContext(lcscsBuilder.create(), getCache(), null, new ByteBufferFactoryImpl(),
                                                 new MarshalledEntryFactoryImpl(null)));
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

            assert !done.await(1, TimeUnit.SECONDS) : "Background thread should have blocked after adding 10 entries";
         } finally {
            underlying.lock.unlock();
         }
      } finally {
         writer.stop();
      }
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
      return BaseStoreTest.mockCache(getClass().getName());
   }
}
