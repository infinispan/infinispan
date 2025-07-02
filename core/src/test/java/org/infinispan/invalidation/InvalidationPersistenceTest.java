package org.infinispan.invalidation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/*
Contributed by Paul Ferraro
 */
@Test(testName = "invalidation.InvalidationPersistenceTest", groups = "functional")
public class InvalidationPersistenceTest extends MultipleCacheManagersTest {

   private static final String NON_TX_CACHE_NAME = "non-tx";
   private static final String PES_TX_CACHE_NAME = "p-tx";
   private static final String OPT_TX_CACHE_NAME = "o-tx";

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(2);
      Configuration nonTx = invalidationConfiguration("a").build();
      cacheManagers.forEach(m -> m.defineConfiguration(NON_TX_CACHE_NAME, nonTx));

      Configuration ptx = invalidationConfiguration("b")
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.PESSIMISTIC)
            .build();
      cacheManagers.forEach(m -> m.defineConfiguration(PES_TX_CACHE_NAME, ptx));

      Configuration otx = invalidationConfiguration("c")
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.OPTIMISTIC)
            .build();
      cacheManagers.forEach(m -> m.defineConfiguration(OPT_TX_CACHE_NAME, otx));

      waitForClusterToForm(NON_TX_CACHE_NAME, PES_TX_CACHE_NAME, OPT_TX_CACHE_NAME);
   }

   private static ConfigurationBuilder invalidationConfiguration(String storeName) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .shared(true)
            .segmented(true)
            .storeName(storeName);
      return builder;
   }

   @DataProvider(name = "caches")
   public static Object[][] cacheNames() {
      return new Object[][]{
            {NON_TX_CACHE_NAME},
            // TODO will be fixed by https://github.com/infinispan/infinispan/issues/15193
            //{OPT_TX_CACHE_NAME},
            //{PES_TX_CACHE_NAME}
      };
   }

   @Test(dataProvider = "caches")
   public void testPut(String cacheName) throws Exception {
      Cache<String, Integer> cache1 = cache(0, cacheName);
      Cache<String, Integer> cache2 = cache(1, cacheName);
      TransactionManager tm1 = cache1.getAdvancedCache().getTransactionManager();
      TransactionManager tm2 = cache2.getAdvancedCache().getTransactionManager();
      Cache<String, Integer> skipLoadCache1 = cache1.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      Cache<String, Integer> skipLoadCache2 = cache2.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      Queue<Map.Entry<String, Listener.Observation>> events1 = new LinkedBlockingQueue<>();
      Queue<Map.Entry<String, Listener.Observation>> events2 = new LinkedBlockingQueue<>();
      Object listener1 = new InvalidationEventCollector(events1);
      Object listener2 = new InvalidationEventCollector(events2);
      String key = "put";
      cache1.addListener(listener1);
      cache2.addListener(listener2);
      try {
         if (tm1 != null) {
            tm1.begin();
         }
         // Initial write
         assertThat(cache1.putIfAbsent(key, 0)).isNull();
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Entry should only exist locally
         assertThat(skipLoadCache1.get(key)).isEqualTo(0);
         assertThat(skipLoadCache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Requires loading of current value, but nothing should be written
         assertThat(cache2.putIfAbsent(key, -1)).isEqualTo(0);
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         // Nothing was updated, there should be no invalidation events
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Entry should have been loaded locally but not invalidated remotely since nothing was written
         assertThat(skipLoadCache1.get(key)).isEqualTo(0);
         assertThat(skipLoadCache2.get(key)).isEqualTo(0);

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires previous value, already available locally
         assertThat(cache2.put(key, 1)).isEqualTo(0);
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Verify that entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(1);

         if (tm1 != null) {
            tm1.begin();
         }
         // Write requires loading current value
         assertThat(cache1.put(key, 2)).isEqualTo(1);
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Verify that entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isEqualTo(2);
         assertThat(skipLoadCache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires loading of current value, to be replaced
         assertThat(cache2.replace(key, 3)).isEqualTo(2);
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(3);

         if (tm2 != null) {
            tm2.begin();
         }
         // Current value already available locally, to be replaced
         assertThat(cache2.replace(key, 4)).isEqualTo(3);
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Entry would already have been invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(4);

         if (tm1 != null) {
            tm1.begin();
         }
         // Requires loading current value, to be removed
         assertThat(cache1.remove(key)).isEqualTo(4);
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();

         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache1.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Requires store lookup, nothing written
         assertThat(cache2.replace(key, 0)).isNull();
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         // Nothing to invalidate
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();

         // Verify no entry in memory
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache1.get(key)).isNull();
         // Verify no entry in store
         assertThat(cache1.get(key)).isNull();
         assertThat(cache2.get(key)).isNull();
      } finally {
         cache1.removeListener(listener1);
         cache2.removeListener(listener2);
      }
   }

   @Test(dataProvider = "caches")
   public void testPutIgnoreReturnValue(String cacheName) throws Exception {
      Cache<String, Integer> cache1 = this.<String, Integer>cache(0, cacheName).getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
      Cache<String, Integer> cache2 = this.<String, Integer>cache(1, cacheName).getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
      TransactionManager tm1 = cache1.getAdvancedCache().getTransactionManager();
      TransactionManager tm2 = cache2.getAdvancedCache().getTransactionManager();
      Cache<String, Integer> skipLoadCache1 = cache1.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      Cache<String, Integer> skipLoadCache2 = cache2.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      Queue<Map.Entry<String, Listener.Observation>> events1 = new LinkedBlockingQueue<>();
      Queue<Map.Entry<String, Listener.Observation>> events2 = new LinkedBlockingQueue<>();
      Object listener1 = new InvalidationEventCollector(events1);
      Object listener2 = new InvalidationEventCollector(events2);
      String key = "put-ignore";
      cache1.addListener(listener1);
      cache2.addListener(listener2);
      try {
         if (tm1 != null) {
            tm1.begin();
         }
         // Initial write
         assertThat(cache1.putIfAbsent(key, 0)).isNull();
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Entry should only exist locally
         assertThat(skipLoadCache1.get(key)).isEqualTo(0);
         assertThat(skipLoadCache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Requires loading current value, but nothing will be written
         assertThat(cache2.putIfAbsent(key, -1)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(0));
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         // Nothing was updated, there should be no invalidation events
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Entry should have been loaded locally but not invalidated remotely since nothing was written
         assertThat(skipLoadCache1.get(key)).isEqualTo(0);
         assertThat(skipLoadCache2.get(key)).isEqualTo(0);

         if (tm2 != null) {
            tm2.begin();
         }
         // Write does not require current value
         assertThat(cache2.put(key, 1)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(0));
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(1);

         if (tm1 != null) {
            tm1.begin();
         }
         // Write does not requiring loading current value
         assertThat(cache1.put(key, 2)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(1));
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isEqualTo(2);
         assertThat(skipLoadCache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires loading current value
         assertThat(cache2.replace(key, 3)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(2));
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(3);

         if (tm2 != null) {
            tm2.begin();
         }
         // Write does not require loading since current value available locally
         assertThat(cache2.replace(key, 4)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(3));
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Remote entry would already have been invalidated
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(4);

         if (tm1 != null) {
            tm1.begin();
         }
         // Write does not require loading current value
         assertThat(cache1.remove(key)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(4));
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache1.get(key)).isNull();
         // Verify store removal
         assertThat(cache1.get(key)).isNull();
         assertThat(cache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Requires store lookup, nothing written
         assertThat(cache2.replace(key, 0)).isNull();
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         // Nothing to invalidate
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();

         // Verify no entry in memory
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache1.get(key)).isNull();
         // Verify no entry in store
         assertThat(cache1.get(key)).isNull();
         assertThat(cache2.get(key)).isNull();
      } finally {
         cache1.removeListener(listener1);
         cache2.removeListener(listener2);
      }
   }

   @Test(dataProvider = "caches")
   public void testCompute(String cacheName) throws Exception {
      Cache<String, Integer> cache1 = cache(0, cacheName);
      Cache<String, Integer> cache2 = cache(1, cacheName);
      TransactionManager tm1 = cache1.getAdvancedCache().getTransactionManager();
      TransactionManager tm2 = cache2.getAdvancedCache().getTransactionManager();
      Cache<String, Integer> skipLoadCache1 = cache1.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      Cache<String, Integer> skipLoadCache2 = cache2.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      Queue<Map.Entry<String, Listener.Observation>> events1 = new LinkedBlockingQueue<>();
      Queue<Map.Entry<String, Listener.Observation>> events2 = new LinkedBlockingQueue<>();
      Object listener1 = new InvalidationEventCollector(events1);
      Object listener2 = new InvalidationEventCollector(events2);
      String key = "compute";
      cache1.addListener(listener1);
      cache2.addListener(listener2);
      try {
         BiFunction<String, Integer, Integer> increment = (k, v) -> (v != null) ? v + 1 : 0;

         if (tm1 != null) {
            tm1.begin();
         }
         // Initial write
         assertThat(cache1.computeIfAbsent(key, k -> 0)).isEqualTo(0);
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Entry should only exist locally
         assertThat(skipLoadCache1.get(key)).isEqualTo(0);
         assertThat(skipLoadCache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Requires loading current value, but nothing written
         assertThat(cache2.computeIfAbsent(key, k -> -1)).isEqualTo(0);
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         // Nothing was updated, there should be no invalidation events
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Value should still be available remotely as nothing was written/invalidated
         assertThat(skipLoadCache1.get(key)).isEqualTo(0);
         assertThat(skipLoadCache2.get(key)).isEqualTo(0);

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires current value already available locally
         assertThat(cache2.compute(key, increment)).isEqualTo(1);
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(1);

         if (tm1 != null) {
            tm1.begin();
         }
         // Write requires loading current value
         assertThat(cache1.compute(key, increment)).isEqualTo(2);
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isEqualTo(2);
         assertThat(skipLoadCache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires loading current value
         assertThat(cache2.computeIfPresent(key, increment)).isEqualTo(3);
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(3);

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires current value, already available locally
         assertThat(cache2.computeIfPresent(key, increment)).isEqualTo(4);
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Entry was already invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(4);

         if (tm1 != null) {
            tm1.begin();
         }
         // Write requires loading current value
         assertThat(cache1.computeIfPresent(key, (k, v) -> null)).isNull();
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache1.get(key)).isNull();
         // Verify store removal
         assertThat(cache1.get(key)).isNull();
         assertThat(cache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Requires store lookup, nothing written
         assertThat(cache2.computeIfPresent(key, (k, v) -> 0)).isNull();
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         // Nothing to invalidate
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();

         // Verify no entry in memory
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache1.get(key)).isNull();
         // Verify no entry in store
         assertThat(cache1.get(key)).isNull();
         assertThat(cache2.get(key)).isNull();
      } finally {
         cache1.removeListener(listener1);
         cache2.removeListener(listener2);
      }
   }

   @Test(dataProvider = "caches")
   public void testComputeIgnoreReturnValue(String cacheName) throws Exception {
      Cache<String, Integer> cache1 = this.<String, Integer>cache(0, cacheName).getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
      Cache<String, Integer> cache2 = this.<String, Integer>cache(1, cacheName).getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
      TransactionManager tm1 = cache1.getAdvancedCache().getTransactionManager();
      TransactionManager tm2 = cache2.getAdvancedCache().getTransactionManager();
      Cache<String, Integer> skipLoadCache1 = cache1.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      Cache<String, Integer> skipLoadCache2 = cache2.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      Queue<Map.Entry<String, Listener.Observation>> events1 = new LinkedBlockingQueue<>();
      Queue<Map.Entry<String, Listener.Observation>> events2 = new LinkedBlockingQueue<>();
      Object listener1 = new InvalidationEventCollector(events1);
      Object listener2 = new InvalidationEventCollector(events2);
      String key = "compute-ignore";
      cache1.addListener(listener1);
      cache2.addListener(listener2);
      try {
         BiFunction<String, Integer, Integer> increment = (k, v) -> (v != null) ? v + 1 : 0;

         if (tm1 != null) {
            tm1.begin();
         }
         // Initial write
         assertThat(cache1.computeIfAbsent(key, k -> 0)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(0));
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Entry should only exist locally
         assertThat(skipLoadCache1.get(key)).isEqualTo(0);
         assertThat(skipLoadCache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires loading current value, but nothing should be written
         assertThat(cache2.computeIfAbsent(key, k -> -1)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(0));
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         // Nothing was updated, there should be no invalidation events
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Entry should still be available remotely since nothing was invalidated
         assertThat(skipLoadCache1.get(key)).isEqualTo(0);
         assertThat(skipLoadCache2.get(key)).isEqualTo(0);

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires current value already available locally
         assertThat(cache2.compute(key, increment)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(1));
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(1);

         if (tm1 != null) {
            tm1.begin();
         }
         // Write requires current value already available locally
         assertThat(cache1.compute(key, increment)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(2));
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Entry already invalidated
         assertThat(skipLoadCache1.get(key)).isEqualTo(2);
         assertThat(skipLoadCache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Write requires loading current value
         assertThat(cache2.computeIfPresent(key, increment)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(3));
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(3);

         if (tm2 != null) {
            tm2.begin();
         }
         // Write required current value, already available locally
         assertThat(cache2.computeIfPresent(key, increment)).satisfiesAnyOf(result -> assertThat(result).isNull(), result -> assertThat(result).isEqualTo(4));
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events1.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();
         // Entry already invalidated
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache2.get(key)).isEqualTo(4);

         if (tm1 != null) {
            tm1.begin();
         }
         // Write requires loading current value
         assertThat(cache1.computeIfPresent(key, (k, v) -> null)).isNull();
         if (tm1 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm1.commit();
         }
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.PRE));
         assertThat(events2.poll()).isEqualTo(Map.entry(key, Listener.Observation.POST));
         assertThat(events2.poll()).isNull();
         // Verify entry was invalidated remotely
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache1.get(key)).isNull();
         // Verify store removal
         assertThat(cache1.get(key)).isNull();
         assertThat(cache2.get(key)).isNull();

         if (tm2 != null) {
            tm2.begin();
         }
         // Requires store lookup, nothing written
         assertThat(cache2.computeIfPresent(key, (k, v) -> 0)).isNull();
         if (tm2 != null) {
            // No invalidation events expected before commit
            assertThat(events1.poll()).isNull();
            assertThat(events2.poll()).isNull();
            tm2.commit();
         }
         // Nothing to invalidate
         assertThat(events1.poll()).isNull();
         assertThat(events2.poll()).isNull();

         // Verify no entry in memory
         assertThat(skipLoadCache1.get(key)).isNull();
         assertThat(skipLoadCache1.get(key)).isNull();
         // Verify no entry in store
         assertThat(cache1.get(key)).isNull();
         assertThat(cache2.get(key)).isNull();
      } finally {
         cache1.removeListener(listener1);
         cache2.removeListener(listener2);
      }
   }

   @Listener
   public static class InvalidationEventCollector {
      private final Collection<Map.Entry<String, Listener.Observation>> events;

      InvalidationEventCollector(Collection<Map.Entry<String, Listener.Observation>> events) {
         this.events = events;
      }

      @CacheEntryInvalidated
      public void invalidated(CacheEntryInvalidatedEvent<String, Integer> event) {
         this.events.add(Map.entry(event.getKey(), event.isPre() ? Listener.Observation.PRE : Listener.Observation.POST));
      }
   }
}
