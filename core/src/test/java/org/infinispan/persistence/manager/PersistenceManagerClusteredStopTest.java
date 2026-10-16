package org.infinispan.persistence.manager;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getFirstStore;
import static org.infinispan.testing.Exceptions.expectRootCause;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.manager.PersistenceManagerLockLeakTest.ControllableStore;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;

/**
 * Stopping a clustered cache with pending store operations.
 *
 * <p>
 * When every store is purged on startup, the other owners keep the data and the store is discarded on the next start.
 * The stop fails the pending callers with {@link IllegalLifecycleStateException} instead of waiting for the store to
 * answer. Otherwise, the stop waits for the store.
 * </p>
 *
 * <p>
 * A single node is enough: the stop mode depends only on the configuration.
 * </p>
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "persistence.manager.PersistenceManagerClusteredStopTest")
public class PersistenceManagerClusteredStopTest extends SingleCacheManagerTest {

   // Guard only: no stop waits for it. Without the fast stop, the stop waits up to distributedSyncTimeout (4 min).
   private static final int STOP_TIMEOUT_SECONDS = 10;
   private static final String NOT_PURGED_CACHE = "not-purged";

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      // The default cache purges its store on startup.
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(storeConfiguration(true));
      cm.defineConfiguration(NOT_PURGED_CACHE, storeConfiguration(false).build());
      return cm;
   }

   @DataProvider(name = "operations")
   public Object[][] operations() {
      return Operation.asDataProvider();
   }

   @Test(dataProvider = "operations")
   public void testFastStopFailsPendingOperation(Operation operation) throws Exception {
      PersistenceManagerImpl pm = persistenceManager(cache);
      ControllableStore store = getFirstStore(cache);
      try {
         // The store never answers on its own.
         store.delayNextOperation();
         CompletionStage<?> pending = operation.perform(cache);
         // Operations through the cache API reach the store asynchronously.
         eventually(pm::anyLocksHeld);

         fork(pm::stop).get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

         expectRootCause(IllegalLifecycleStateException.class,
               () -> pending.toCompletableFuture().get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
         assertFalse(pm.anyLocksHeld(), "No lock should be held after the stop");
      } finally {
         store.endDelay();
      }
   }

   public void testFastStopEndsOpenPublisher() throws Exception {
      PersistenceManagerImpl pm = persistenceManager(cache);
      // Requests a single entry and never asks for more, leaving the stream open.
      TestSubscriber<MarshallableEntry<Object, Object>> subscriber = new TestSubscriber<>(1);
      try {
         for (int i = 0; i < 10; i++) {
            cache.put("k" + i, "v" + i);
         }
         Flowable.fromPublisher(pm.publishEntries(true, true)).subscribe(subscriber);
         subscriber.awaitCount(1);

         fork(pm::stop).get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

         // The subscriber learns that the stream ended because of the stop, instead of waiting forever.
         subscriber.awaitDone(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
         subscriber.assertValueCount(1);
         subscriber.assertError(IllegalLifecycleStateException.class);
         assertFalse(pm.anyLocksHeld(), "No lock should be held after the stop");
      } finally {
         subscriber.cancel();
      }
   }

   public void testGracefulStopFinishesAfterPendingWrite() throws Exception {
      Cache<Object, Object> notPurged = cacheManager.getCache(NOT_PURGED_CACHE);
      PersistenceManagerImpl pm = persistenceManager(notPurged);
      ControllableStore store = getFirstStore(notPurged);
      try {
         store.delayNextOperation();
         Operation.WRITE.perform(notPurged);

         Future<Void> stop = fork(pm::stop);
         store.endDelay();

         stop.get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } finally {
         // No-op once the delay ended, here in case the test fails before that.
         store.endDelay();
      }
   }

   private static ConfigurationBuilder storeConfiguration(boolean purgeOnStartup) {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfg.persistence().addStore(ControllableStore.ConfigurationBuilder.class).purgeOnStartup(purgeOnStartup);
      return cfg;
   }

   private static PersistenceManagerImpl persistenceManager(Cache<Object, Object> cache) {
      return (PersistenceManagerImpl) extractComponent(cache, PersistenceManager.class);
   }

   /**
    * An operation that holds the persistence read lock while the store has not answered.
    */
   public enum Operation {
      /**
       * Single-key write, tracked until the store answers.
       */
      WRITE {
         @Override
         CompletionStage<?> perform(Cache<Object, Object> cache) {
            KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
            String key = "k";
            return persistenceManager(cache).writeToAllNonTxStores(
                  MarshalledEntryUtil.create(key, "v", cache), keyPartitioner.getSegment(key), BOTH);
         }
      },
      /**
       * Batch write holding the lock for the whole batch, such as the passivation of all entries.
       */
      BATCH_WRITE {
         @Override
         CompletionStage<?> perform(Cache<Object, Object> cache) {
            List<MarshallableEntry<Object, Object>> entries = List.of(
                  MarshalledEntryUtil.create("k1", "v1", cache),
                  MarshalledEntryUtil.create("k2", "v2", cache));
            return persistenceManager(cache).writeEntries(entries, BOTH);
         }
      },
      /**
       * Batch write through the cache API.
       */
      PUT_ALL {
         @Override
         CompletionStage<?> perform(Cache<Object, Object> cache) {
            // Skips the load of the previous values, so the pending store operation is the batch write.
            return cache.getAdvancedCache()
                  .withFlags(Flag.SKIP_CACHE_LOAD)
                  .putAllAsync(Map.of("k1", "v1", "k2", "v2"));
         }
      },
      /**
       * Batch remove through the cache API.
       */
      REMOVE_ALL {
         @Override
         CompletionStage<?> perform(Cache<Object, Object> cache) {
            // RemoveAllCommand never loads the previous values, so the pending store operation is the batch remove.
            return cache.removeAllAsync(Set.of("k1", "k2"));
         }
      };

      /**
       * Starts the operation against the given cache.
       *
       * @param cache The cache to operate on.
       * @return The stage of the caller, completing once the store answers.
       */
      abstract CompletionStage<?> perform(Cache<Object, Object> cache);

      static Object[][] asDataProvider() {
         Operation[] values = values();
         Object[][] params = new Object[values.length][];
         for (int i = 0; i < values.length; i++) {
            params[i] = new Object[]{values[i]};
         }
         return params;
      }
   }
}
