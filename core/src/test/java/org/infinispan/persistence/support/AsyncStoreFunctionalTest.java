package org.infinispan.persistence.support;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.async.AsyncCacheWriter;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.Remove;
import org.infinispan.persistence.modifications.Store;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Functional tests of the async store when running associated with a cache instance.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "persistence.decorators.AsyncStoreFunctionalTest")
public class AsyncStoreFunctionalTest {

   private static final Log log = LogFactory.getLog(AsyncStoreFunctionalTest.class);

   public void testPutAfterPassivation() {
      ConfigurationBuilder builder = asyncStoreWithEvictionBuilder();
      builder.persistence().passivation(true);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            // Hack the component metadata repository
            // to inject the custom cache loader manager
            GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cm);
            gcr.getComponentMetadataRepo().injectFactoryForComponent(
                  PersistenceManager.class, CustomCacheLoaderManagerFactory.class);

            Cache<Integer, String> cache = cm.getCache();

            MockAsyncCacheWriter cacheStore = TestingUtil.getFirstWriter(cache);
            CountDownLatch modApplyLatch = cacheStore.modApplyLatch;
            CountDownLatch lockedWaitLatch = cacheStore.lockedWaitLatch;

            // Store an entry in the cache
            cache.put(1, "v1");
            // Store a second entry to force the previous entry
            // to be evicted and passivated
            cache.put(2, "v2");

            try {
               // Wait for async store to have this modification queued up,
               // ready to apply it to the cache store...
               log.trace("Wait for async store to lock keys");
               lockedWaitLatch.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }

            try {
               // Even though it's in the process of being passivated,
               // the entry should still be found in memory
               assertEquals("v1", cache.get(1));
            } finally {
               modApplyLatch.countDown();
            }
         }
      });
   }

   public void testPutAfterEviction() {
      ConfigurationBuilder builder = asyncStoreWithEvictionBuilder();

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            // Hack the component metadata repository
            // to inject the custom cache loader manager
            GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cm);
            gcr.getComponentMetadataRepo().injectFactoryForComponent(
                  PersistenceManager.class, CustomCacheLoaderManagerFactory.class);

            Cache<Integer, String> cache = cm.getCache();

            MockAsyncCacheWriter cacheStore = TestingUtil.getFirstWriter(cache);
            CountDownLatch modApplyLatch = cacheStore.modApplyLatch;
            CountDownLatch lockedWaitLatch = cacheStore.lockedWaitLatch;

            // Store an entry in the cache
            cache.put(1, "v1");

            try {
               // Wait for async store to have this modification queued up,
               // ready to apply it to the cache store...
               log.trace("Wait for async store to lock keys");
               lockedWaitLatch.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }

            // This shouldn't result in k=1 being evicted
            // because the k=1 put is queued in the async store
            cache.put(2, "v2");

            try {
               assertEquals("v1", cache.get(1));
               assertEquals("v2", cache.get(2));
            } finally {
               modApplyLatch.countDown();
            }
         }
      });
   }

   public void testGetAfterRemove() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .async().enabled(true);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            // Hack the component metadata repository
            // to inject the custom cache loader manager
            GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cm);
            gcr.getComponentMetadataRepo().injectFactoryForComponent(
                  PersistenceManager.class, CustomCacheLoaderManagerFactory.class);

            Cache<Integer, String> cache = cm.getCache();

            MockAsyncCacheWriter cacheStore = TestingUtil.getFirstWriter(cache);
            CountDownLatch modApplyLatch = cacheStore.modApplyLatch;
            CountDownLatch lockedWaitLatch = cacheStore.lockedWaitLatch;

            // Store a value first
            cache.put(1, "skip");

            // Wait until cache store contains the expected key/value pair
            ((DummyInMemoryStore) cacheStore.undelegate())
                  .blockUntilCacheStoreContains(1, "skip", 60000);

            // Remove it from the cache container
            cache.remove(1);

            try {
               // Wait for async store to have this modification queued up,
               // ready to apply it to the cache store...
               lockedWaitLatch.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }

            try {
               // Even though the remove it's pending,
               // the entry should not be retrieved
               assertEquals(null, cache.get(1));
            } finally {
               modApplyLatch.countDown();
            }

            DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
            dataContainer.purgeExpired();

            Set<Integer> keys = cache.keySet();
            assertTrue("Keys not empty: " + keys, keys.isEmpty());
            Set<Map.Entry<Integer, String>> entries = cache.entrySet();
            assertTrue("Entry set not empty: " + entries, entries.isEmpty());
            Collection<String> values = cache.values();
            assertTrue("Values not empty: " + values, values.isEmpty());
         }
      });
   }

   private ConfigurationBuilder asyncStoreWithEvictionBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // Emulate eviction with direct data container eviction
      builder.eviction().strategy(EvictionStrategy.LRU).maxEntries(1)
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .async().enabled(true);
      return builder;
   }

   public static class MockAsyncCacheWriter extends AsyncCacheWriter {

      private static final Log log = LogFactory.getLog(MockAsyncCacheWriter.class);

      private final CountDownLatch modApplyLatch;
      private final CountDownLatch lockedWaitLatch;

      public MockAsyncCacheWriter(CountDownLatch modApplyLatch, CountDownLatch lockedWaitLatch,
                                  CacheWriter delegate) {
         super(delegate);
         this.modApplyLatch = modApplyLatch;
         this.lockedWaitLatch = lockedWaitLatch;
      }

      @Override
      protected void applyModificationsSync(List<Modification> mods)
            throws PersistenceException {
         try {
            // Wait for signal to do the modification
            if (containsModificationForKey(1, mods) && !isSkip(findModificationForKey(1, mods))) {
               log.tracef("Wait to apply modifications: %s", mods);
               lockedWaitLatch.countDown();
               modApplyLatch.await(60, TimeUnit.SECONDS);
               log.tracef("Apply modifications: %s", mods);
            }
            super.applyModificationsSync(mods);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }

      private boolean containsModificationForKey(Object key, List<Modification> mods) {
         return findModificationForKey(key, mods) != null;
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

      private boolean isSkip(Modification mod) {
         if (mod instanceof Store) {
            MarshalledEntry storedValue = ((Store) mod).getStoredValue();
            return storedValue.getValue().equals("skip");
         }
         return false;
      }

   }

   public static class CustomCacheLoaderManagerFactory
         extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

      @Override
      public <T> T construct(Class<T> componentType) {
         return (T) new CustomPersistenceManager();
      }

   }

   public static class CustomPersistenceManager extends PersistenceManagerImpl {

      @Override
      protected AsyncCacheWriter createAsyncWriter(CacheWriter tmpStore) {
         CountDownLatch modApplyLatch = new CountDownLatch(1);
         CountDownLatch lockedWaitLatch = new CountDownLatch(1);
         return new MockAsyncCacheWriter(modApplyLatch, lockedWaitLatch, tmpStore);
      }
   }

}
