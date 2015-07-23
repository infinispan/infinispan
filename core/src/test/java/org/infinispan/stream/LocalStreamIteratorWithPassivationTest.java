package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.Immutables;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Test to verify distributed entry behavior when a loader with passivation is present in local mode
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.LocalStreamIteratorWithPassivationTest")
public class LocalStreamIteratorWithPassivationTest extends DistributedStreamIteratorWithPassivationTest {
   protected final static String CACHE_NAME = "LocalStreamIteratorWithPassivationTest";
   protected ConfigurationBuilder builderUsed;

   public LocalStreamIteratorWithPassivationTest() {
      super(false, CacheMode.LOCAL);
   }

   @Test(enabled = false, description = "This requires supporting concurrent activation in cache loader interceptor")
   public void testConcurrentActivation() throws InterruptedException, ExecutionException, TimeoutException {
      final Cache<String, String> cache = cache(0, CACHE_NAME);

      Map<String, String> originalValues = new HashMap<>();
      originalValues.put(cache.toString() + 1, "cache0");
      originalValues.put(cache.toString() + 2, "cache1");
      originalValues.put(cache.toString() + 3, "cache2");

      final String loaderKey = cache.toString() + " in loader";
      final String loaderValue = "loader0";

      cache.putAll(originalValues);

      // Put this in after the cache has been updated
      originalValues.put(loaderKey, loaderValue);

      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache, PersistenceManager.class);
      DummyInMemoryStore store = persistenceManager.getStores(DummyInMemoryStore.class).iterator().next();

      TestObjectStreamMarshaller sm = new TestObjectStreamMarshaller();
      PersistenceManager pm = null;
      try {
         store.write(new MarshalledEntryImpl(loaderKey, loaderValue, null, sm));

         final CheckPoint checkPoint = new CheckPoint();
         pm = waitUntilAboutToProcessStoreTask(cache, checkPoint);

         Future<Void> future = fork(() -> {
            // Wait until loader is invoked
            checkPoint.awaitStrict("pre_process_on_all_stores_invoked", 10, TimeUnit.SECONDS);

            // Now force the entry to be moved to the in memory
            assertEquals(loaderValue, cache.get(loaderKey));

            checkPoint.triggerForever("pre_process_on_all_stores_released");
            return null;
         });

         Iterator<Map.Entry<String, String>> iterator = cache.entrySet().stream().iterator();

         // we need this count since the map will replace same key'd value
         int count = 0;
         Map<String, String> results = new HashMap<>();
         while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            results.put(entry.getKey(), entry.getValue());
            count++;
         }
         assertEquals(count, 4);
         assertEquals(originalValues, results);

         future.get(10, TimeUnit.SECONDS);
      } finally {
         if (pm != null) {
            TestingUtil.replaceComponent(cache, PersistenceManager.class, pm, true);
         }
         sm.stop();
      }
   }
   
   @Test
   public void testConcurrentActivationWithFilter() throws InterruptedException, ExecutionException, TimeoutException {
      final Cache<String, String> cache = cache(0, CACHE_NAME);

      Map<String, String> originalValues = new HashMap<>();
      originalValues.put(cache.toString() + 1, "cache0");
      originalValues.put(cache.toString() + 2, "cache1");
      originalValues.put(cache.toString() + 3, "cache2");

      final String loaderKey = cache.toString() + " in loader";
      final String loaderValue = "loader0";

      cache.putAll(originalValues);

      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache, PersistenceManager.class);
      DummyInMemoryStore store = persistenceManager.getStores(DummyInMemoryStore.class).iterator().next();

      TestObjectStreamMarshaller sm = new TestObjectStreamMarshaller();
      PersistenceManager pm = null;
      try {
         store.write(new MarshalledEntryImpl(loaderKey, loaderValue, null, sm));

         final CheckPoint checkPoint = new CheckPoint();
         pm = waitUntilAboutToProcessStoreTask(cache, checkPoint);

         Future<Void> future = fork(() -> {
            // Wait until loader is invoked
            checkPoint.awaitStrict("pre_process_on_all_stores_invoked", 10, TimeUnit.SECONDS);

            // Now force the entry to be moved to the in memory
            assertEquals(loaderValue, cache.get(loaderKey));

            checkPoint.triggerForever("pre_process_on_all_stores_released");
            return null;
         });

         Iterator<CacheEntry<String, String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream().filter(
                 CacheFilters.predicate(new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(Immutables
                         .immutableSetCopy(originalValues.keySet()), true)))).iterator();

         // we need this count since the map will replace same key'd value
         int count = 0;
         Map<String, String> results = new HashMap<>();
         while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            results.put(entry.getKey(), entry.getValue());
            count++;
         }
         // We shouldn't have found the value in the loader
         assertEquals(count, 3);
         assertEquals(originalValues, results);

         future.get(10, TimeUnit.SECONDS);
      } finally {
         if (pm != null) {
            TestingUtil.replaceComponent(cache, PersistenceManager.class, pm, true);
         }
         sm.stop();
      }
   }
   
   @Test(enabled = false, description = "This requires supporting concurrent activation in cache loader interceptor")
   public void testConcurrentActivationWithConverter() throws InterruptedException, ExecutionException, TimeoutException {
      final Cache<String, String> cache = cache(0, CACHE_NAME);

      Map<String, String> originalValues = new HashMap<>();
      originalValues.put(cache.toString() + 1, "cache0");
      originalValues.put(cache.toString() + 2, "cache1");
      originalValues.put(cache.toString() + 3, "cache2");

      final String loaderKey = cache.toString() + " in loader";
      final String loaderValue = "loader0";

      cache.putAll(originalValues);
      
      // Put this in after the cache has been updated
      originalValues.put(loaderKey, loaderValue);

      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache, PersistenceManager.class);
      DummyInMemoryStore store = persistenceManager.getStores(DummyInMemoryStore.class).iterator().next();

      TestObjectStreamMarshaller sm = new TestObjectStreamMarshaller();
      PersistenceManager pm = null;
      try {
         store.write(new MarshalledEntryImpl(loaderKey, loaderValue, null, sm));

         final CheckPoint checkPoint = new CheckPoint();
         pm = waitUntilAboutToProcessStoreTask(cache, checkPoint);

         Future<Void> future = fork(() -> {
            // Wait until loader is invoked
            checkPoint.awaitStrict("pre_process_on_all_stores_invoked", 10, TimeUnit.SECONDS);

            // Now force the entry to be moved to the in memory
            assertEquals(loaderValue, cache.get(loaderKey));

            checkPoint.triggerForever("pre_process_on_all_stores_released");
            return null;
         });

         Iterator<CacheEntry<String, String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream().map(
                 CacheFilters.function(new StringTruncator(1, 3))).iterator();

         // we need this count since the map will replace same key'd value
         int count = 0;
         Map<String, String> results = new HashMap<>();
         while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            results.put(entry.getKey(), entry.getValue());
            count++;
         }
         // We shouldn't have found the value in the loader
         assertEquals(count, 4);
         for (Map.Entry<String, String> entry : originalValues.entrySet()) {
            assertEquals(entry.getValue().substring(1, 4), results.get(entry.getKey()));
         }

         future.get(10, TimeUnit.SECONDS);
      } finally {
         if (pm != null) {
            TestingUtil.replaceComponent(cache, PersistenceManager.class, pm, true);
         }
         sm.stop();
      }
   }


   protected PersistenceManager waitUntilAboutToProcessStoreTask(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      PersistenceManager pm = TestingUtil.extractComponent(cache, PersistenceManager.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(pm);
      PersistenceManager mockManager = mock(PersistenceManager.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_process_on_all_stores_invoked");
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_process_on_all_stores_released", 10, TimeUnit.SECONDS);

            return forwardedAnswer.answer(invocation);
         }
      }).when(mockManager).processOnAllStores(any(Executor.class),      any(KeyFilter.class), any(AdvancedCacheLoader.CacheLoaderTask.class),
                                                  anyBoolean(), anyBoolean());
      TestingUtil.replaceComponent(cache, PersistenceManager.class, mockManager, true);
      return pm;
   }

   /**
    * This test is to verify that if a concurrent passivation occurs while switching between data container and loader(s)
    * that we don't return the same key/value twice
    * @throws InterruptedException
    * @throws ExecutionException
    * @throws TimeoutException
    */
   @Test
   public void testConcurrentPassivation() throws InterruptedException, ExecutionException, TimeoutException {
      final Cache<String, String> cache = cache(0, CACHE_NAME);

      Map<String, String> originalValues = new HashMap<>();
      originalValues.put(cache.toString() + 1, "cache0");
      originalValues.put(cache.toString() + 2, "cache1");
      originalValues.put(cache.toString() + 3, "cache2");

      final String loaderKey = cache.toString() + " loader-value";
      final String loaderValue = "loader0";

      // Make sure this is in the cache to begin with
      originalValues.put(loaderKey, loaderValue);

      cache.putAll(originalValues);

      PersistenceManager pm = null;

      try {
         final CheckPoint checkPoint = new CheckPoint();
         pm = waitUntilAboutToProcessStoreTask(cache, checkPoint);

         Future<Void> future = fork(() -> {
            // Wait until loader is invoked
            checkPoint.awaitStrict("pre_process_on_all_stores_invoked", 1000, TimeUnit.SECONDS);

            // Now force the entry to be moved to loader
            TestingUtil.extractComponent(cache, PassivationManager.class).passivate(new ImmortalCacheEntry(loaderKey, loaderValue));

            checkPoint.triggerForever("pre_process_on_all_stores_released");
            return null;
         });

         Iterator<Map.Entry<String, String>> iterator = cache.entrySet().stream().iterator();

         // we need this count since the map will replace same key'd value
         int count = 0;
         Map<String, String> results = new HashMap<>();
         while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            results.put(entry.getKey(), entry.getValue());
            count++;
         }
         assertEquals(4, count);
         assertEquals(originalValues, results);

         future.get(10, TimeUnit.SECONDS);
      } finally {
         if (pm != null) {
            TestingUtil.replaceComponent(cache, PersistenceManager.class, pm, true);
         }
      }
   }
}

