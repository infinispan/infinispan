/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.loaders.decorators;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheLoaderManagerImpl;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
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
@Test(groups = "functional", testName = "loaders.RemovedEntryFoundAsyncStoreTest")
public class AsyncStoreFunctionalTest {

   private static final Log log = LogFactory.getLog(AsyncStoreFunctionalTest.class);

   public void testPutAfterPassivation() {
      ConfigurationBuilder builder = asyncStoreWithEvictionBuilder();
      builder.loaders().passivation(true);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            // Hack the component metadata repository
            // to inject the custom cache loader manager
            GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cm);
            gcr.getComponentMetadataRepo().injectFactoryForComponent(
                  CacheLoaderManager.class, CustomCacheLoaderManagerFactory.class);

            Cache<Integer, String> cache = cm.getCache();

            MockAsyncStore cacheStore = getMockAsyncStore(cache);
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
                  CacheLoaderManager.class, CustomCacheLoaderManagerFactory.class);

            Cache<Integer, String> cache = cm.getCache();

            MockAsyncStore cacheStore = getMockAsyncStore(cache);
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
      builder.loaders()
               .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class)
               .async().enabled(true);

      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            // Hack the component metadata repository
            // to inject the custom cache loader manager
            GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cm);
            gcr.getComponentMetadataRepo().injectFactoryForComponent(
                  CacheLoaderManager.class, CustomCacheLoaderManagerFactory.class);

            Cache<Integer, String> cache = cm.getCache();

            MockAsyncStore cacheStore = getMockAsyncStore(cache);
            CountDownLatch modApplyLatch = cacheStore.modApplyLatch;
            CountDownLatch lockedWaitLatch = cacheStore.lockedWaitLatch;

            // Store a value first
            cache.put(1, "skip");

            // Wait until cache store contains the expected key/value pair
            ((DummyInMemoryCacheStore) cacheStore.getDelegate())
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

   private MockAsyncStore getMockAsyncStore(Cache<Integer, String> cache) {
      CustomCacheLoaderManager cacheLoaderManager = (CustomCacheLoaderManager)
            TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      return (MockAsyncStore)
            cacheLoaderManager.getCacheStore();
   }

   private ConfigurationBuilder asyncStoreWithEvictionBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // Emulate eviction with direct data container eviction
      builder.eviction().strategy(EvictionStrategy.LRU).maxEntries(1)
            .loaders()
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class)
            .async().enabled(true);
      return builder;
   }

   public static class MockAsyncStore extends AsyncStore {

      private static final Log log = LogFactory.getLog(MockAsyncStore.class);

      private final CountDownLatch modApplyLatch;
      private final CountDownLatch lockedWaitLatch;

      public MockAsyncStore(CountDownLatch modApplyLatch, CountDownLatch lockedWaitLatch,
            CacheStore delegate, AsyncStoreConfig asyncStoreConfig) {
         super(delegate, asyncStoreConfig);
         this.modApplyLatch = modApplyLatch;
         this.lockedWaitLatch = lockedWaitLatch;
      }

      @Override
      protected void applyModificationsSync(List<Modification> mods)
            throws CacheLoaderException {
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

      private boolean isSkip(Modification mod) {
         if (mod instanceof Store) {
            InternalCacheEntry storedEntry = ((Store) mod).getStoredEntry();
            return storedEntry.getValue().equals("skip");
         }
         return false;
      }

   }

   public static class CustomCacheLoaderManagerFactory
         extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

      @Override
      public <T> T construct(Class<T> componentType) {
         return (T) new CustomCacheLoaderManager();
      }

   }

   public static class CustomCacheLoaderManager extends CacheLoaderManagerImpl {

      @Override
      protected AsyncStore createAsyncStore(CacheStore tmpStore, CacheStoreConfig cfg2) {
         CountDownLatch modApplyLatch = new CountDownLatch(1);
         CountDownLatch lockedWaitLatch = new CountDownLatch(1);
         return new MockAsyncStore(modApplyLatch, lockedWaitLatch,
               tmpStore, cfg2.getAsyncStoreConfig());
      }

   }

}
