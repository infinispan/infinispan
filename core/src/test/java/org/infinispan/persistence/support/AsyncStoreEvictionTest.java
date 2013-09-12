package org.infinispan.persistence.support;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

@Test(groups = "unit", testName = "persistence.decorators.AsyncStoreEvictionTest")
public class AsyncStoreEvictionTest {

   // set to false to fix all the tests
   private static final boolean USE_ASYNC_STORE = true;

   private static ConfigurationBuilder config(boolean passivation, int threads) {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.expiration().wakeUpInterval(100);
      config.eviction().maxEntries(1).strategy(EvictionStrategy.LRU);
      config.persistence()
         .passivation(passivation)
         .addStore(LockableStoreConfigurationBuilder.class)
         .async()
            .enabled(USE_ASYNC_STORE)
            .threadPoolSize(threads);
      return config;
   }

   private final static ThreadLocal<LockableStore> STORE = new ThreadLocal<LockableStore>();


   public static class LockableStoreConfigurationBuilder extends DummyInMemoryStoreConfigurationBuilder {
      public LockableStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder);
      }

      @Override
      public LockableStoreConfiguration create() {
         return new LockableStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications,
                                                          async.create(), singletonStore.create(), preload, shared, properties, debug, slow, storeName, failKey);
      }
   }

   @ConfigurationFor(LockableStore.class)
   @BuiltBy(LockableStoreConfigurationBuilder.class)
   public static class LockableStoreConfiguration extends DummyInMemoryStoreConfiguration {

      public LockableStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties, boolean debug, boolean slow, String storeName, Object failKey) {
         super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties, debug, slow, storeName, failKey);
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

   private static abstract class CacheCallable extends CacheManagerCallable {
      protected final Cache<String, String> cache;
      protected final LockableStore store;

      CacheCallable(ConfigurationBuilder builder) {
         super(TestCacheManagerFactory.createCacheManager(builder));
         cache = cm.getCache();
         store = STORE.get();
      }
   }

   public void testEndToEndEvictionPassivation() throws Exception {
      testEndToEndEviction(true);
   }
   public void testEndToEndEviction() throws Exception {
      testEndToEndEviction(false);
   }
   private void testEndToEndEviction(boolean passivation) throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(passivation, 1)) {
         @Override
         public void call() {
            // simulate slow back end store
            store.lock.lock();
            try {
               cache.put("k1", "v1");
               cache.put("k2", "v2"); // force eviction of "k1"
               TestingUtil.sleepThread(100); // wait until the only AsyncProcessor thread is blocked
               cache.put("k3", "v3");
               cache.put("k4", "v4"); // force eviction of "k3"

               assert "v3".equals(cache.get("k3")) : "cache must return k3 == v3 (was: " + cache.get("k3") + ")";
            } finally {
               store.lock.unlock();
            }
         }
      });
   }

   public void testEndToEndUpdatePassivation() throws Exception {
      testEndToEndUpdate(true);
   }
   public void testEndToEndUpdate() throws Exception {
      testEndToEndUpdate(false);
   }
   private void testEndToEndUpdate(boolean passivation) throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(passivation, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v0");
            cache.put("k2", "v2"); // force eviction of "k1"

            // wait for k1 == v1 to appear in store
            while (store.load("k1") == null)
               TestingUtil.sleepThread(10);

            // simulate slow back end store
            store.lock.lock();
            try {
               cache.put("k3", "v3");
               cache.put("k4", "v4"); // force eviction of "k3"
               TestingUtil.sleepThread(100); // wait until the only AsyncProcessor thread is blocked
               cache.put("k1", "v1");
               cache.put("k5", "v5"); // force eviction of "k1"

               assert "v1".equals(cache.get("k1")) : "cache must return k1 == v1 (was: " + cache.get("k1") + ")";
            } finally {
               store.lock.unlock();
            }
         }
      });
   }

   public void testEndToEndRemovePassivation() throws Exception {
      testEndToEndRemove(true);
   }
   public void testEndToEndRemove() throws Exception {
      testEndToEndRemove(false);
   }
   private void testEndToEndRemove(boolean passivation) throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(passivation, 2)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.put("k2", "v2"); // force eviction of "k1"

            // wait for "k1" to appear in store
            while (store.load("k1") == null)
               TestingUtil.sleepThread(10);

            // simulate slow back end store
            store.lock.lock();
            try {
               cache.remove("k1");
               TestingUtil.sleepThread(100); // wait until the first AsyncProcessor thread is blocked
               cache.remove("k1"); // make second AsyncProcessor thread burn asyncProcessorIds
               TestingUtil.sleepThread(200); // wait for reaper to collect InternalNullEntry

               assert null == cache.get("k1") : "cache must return k1 == null (was: " + cache.get("k1") + ")";
            } finally {
               store.lock.unlock();
            }
         }
      });
   }

   public void testNPE() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.remove("k1");
            // this causes NPE in AsyncCacheWriter.isLocked(InternalNullEntry.getKey())
            cache.put("k2", "v2");
         }
      });
   }

   public void testLIRS() throws Exception {
      ConfigurationBuilder config = config(false, 1);
      config.eviction().strategy(EvictionStrategy.LIRS).maxEntries(1);
      TestingUtil.withCacheManager(new CacheCallable(config) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            cache.put("k1", "v3");
            cache.put("k2", "v4");
            cache.put("k3", "v3");
            cache.put("k4", "v4");
         }
      });
   }

   public void testSize() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.put("k2", "v2");

            assertEquals("cache size must be 1", 1, cache.getAdvancedCache().getDataContainer().size());
         }
      });
   }

   public void testSizeAfterExpiration() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.put("k2", "v2");
            TestingUtil.sleepThread(200);

            assertFalse("expiry doesn't work even after expiration", 2 == cache.getAdvancedCache().getDataContainer().size());
         }
      });
   }

   public void testSizeAfterEvict() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.evict("k1");

            assertEquals("cache size must be 0", 0, cache.getAdvancedCache().getDataContainer().size());
         }
      });
   }

   public void testSizeAfterRemove() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.remove("k1");

            assertEquals("cache size must be 0", 0, cache.getAdvancedCache().getDataContainer().size());
         }
      });
   }

   public void testSizeAfterRemoveAndExpiration() throws Exception {
      TestingUtil.withCacheManager(new CacheCallable(config(false, 1)) {
         @Override
         public void call() {
            cache.put("k1", "v1");
            cache.remove("k1");
            int size = cache.size();
            TestingUtil.sleepThread(200);

            assertFalse("remove only works after expiration", size == 1 && cache.size() == 0);
         }
      });
   }
}
