package org.infinispan.xsite.irac;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.RequestUUID;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests for keys tracked by {@link IracManager} whose entry and tombstone are both missing.
 * <p>
 * The entry can be temporarily missing while passivation is in progress after an eviction and, in that case, the key
 * must be retried, after the back-off, for as long as needed. However, the key must not be retried when the entry can
 * never be loaded, for example, when it is evicted from a cache without a store to load it from.
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "xsite.irac.IracMissingEntryTest")
public class IracMissingEntryTest extends MultipleCacheManagersTest {

   private static final String SITE_NAME = "LON-1";
   private static final String REMOTE_SITE_NAME = "NYC";
   private static final String CACHE_NO_STORE = "irac-no-store";
   private static final String CACHE_WITH_STORE = "irac-with-store";
   private static final String CACHE_EVICTION = "irac-eviction";
   private static final String CACHE_WRITE_ONLY_STORE = "irac-write-only-store";
   private static final String CACHE_SINGLE_KEY_BATCH = "irac-single-key-batch";
   private static final int MAX_COUNT = 10;
   private static final int NUMBER_OF_KEYS = 50;
   private static final int NUMBER_OF_HEALTHY_KEYS = 10;

   private final AtomicLong requestId = new AtomicLong();
   private final List<Runnable> cleanupTasks = new ArrayList<>(2);
   private final ControlledExponentialBackOff backOff = new ControlledExponentialBackOff(CACHE_WITH_STORE);

   @Override
   protected void createCacheManagers() throws Throwable {
      TransportFlags flags = new TransportFlags().withSiteIndex(0).withSiteName(SITE_NAME).withFD(true);
      createClusteredCaches(2, CACHE_NO_STORE, configurationWithoutStore(), flags);
      defineConfigurationOnAllManagers(CACHE_WITH_STORE, configurationWithStore());
      waitForClusterToForm(CACHE_WITH_STORE);
      defineConfigurationOnAllManagers(CACHE_EVICTION, configurationWithEviction());
      waitForClusterToForm(CACHE_EVICTION);
      defineConfigurationOnAllManagers(CACHE_WRITE_ONLY_STORE, configurationWithWriteOnlyStore());
      waitForClusterToForm(CACHE_WRITE_ONLY_STORE);
      defineConfigurationOnAllManagers(CACHE_SINGLE_KEY_BATCH, configurationWithSingleKeyBatch());
      waitForClusterToForm(CACHE_SINGLE_KEY_BATCH);
      for (String cacheName : List.of(CACHE_NO_STORE, CACHE_WITH_STORE, CACHE_EVICTION, CACHE_WRITE_ONLY_STORE, CACHE_SINGLE_KEY_BATCH)) {
         for (Cache<?, ?> cache : caches(cacheName)) {
            // nothing is sent to the remote site; the keys are removed from the queue as if they were sent.
            extractComponent(cache, TakeOfflineManager.class).takeSiteOffline(REMOTE_SITE_NAME);
         }
      }
      // the retries of a cache with a store wait for the back-off, controlled by the tests.
      for (String cacheName : List.of(CACHE_WITH_STORE, CACHE_SINGLE_KEY_BATCH)) {
         for (Cache<?, ?> cache : caches(cacheName)) {
            ((DefaultIracManager) extractComponent(cache, IracManager.class)).setBackOff(backup -> backOff);
         }
      }
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      cleanupTasks.forEach(Runnable::run);
      cleanupTasks.clear();
      // unblocks any retry still waiting, so the next test starts without a back-off in progress.
      backOff.release();
      backOff.cleanupEvents();
      super.clearContent();
   }

   /**
    * A cache without a store can never load the entry back; the key must be discarded instead of being retried.
    */
   public void testMissingEntryWithoutStoreIsDiscarded() {
      Cache<Object, Object> cache = cache(0, CACHE_NO_STORE);
      IracManager iracManager = extractComponent(cache, IracManager.class);
      String key = getStringKeyForCache("missing-without-store", cache);
      CountingEntryLoader entryLoader = wrapEntryLoader(cache);

      trackKey(cache, key);

      eventually("The key was never discarded", () -> !iracManager.containsKey(key));
      assertEquals(1, entryLoader.loadCount(key), "The entry must not be loaded more than once");
   }

   /**
    * Entries are never loaded from a write-only store, so a cache whose only store is write-only can never load the
    * entry back either; the key must be discarded instead of being retried.
    */
   public void testMissingEntryWithWriteOnlyStoreIsDiscarded() {
      Cache<Object, Object> cache = cache(0, CACHE_WRITE_ONLY_STORE);
      IracManager iracManager = extractComponent(cache, IracManager.class);
      String key = getStringKeyForCache("missing-write-only-store", cache);
      CountingEntryLoader entryLoader = wrapEntryLoader(cache);

      trackKey(cache, key);

      eventually("The key was never discarded", () -> !iracManager.containsKey(key));
      assertEquals(1, entryLoader.loadCount(key), "The entry must not be loaded more than once");
   }

   /**
    * The entry may not be loadable while passivation is in progress after an eviction. The key must be retried, but
    * only after the backup's back-off, instead of immediately.
    */
   public void testMissingEntryWithStoreWaitsForBackOff() {
      Cache<Object, Object> cache = cache(0, CACHE_WITH_STORE);
      IracManager iracManager = extractComponent(cache, IracManager.class);
      String key = getStringKeyForCache("missing-with-store", cache);
      cache.put(key, "value");
      eventually("The key was not sent", () -> !iracManager.containsKey(key));
      backOff.cleanupEvents();

      CountingEntryLoader entryLoader = wrapEntryLoader(cache);
      // the entry is in the store, but the first load does not find it, as if the passivation is still in progress.
      entryLoader.failToLoad(key, 1);

      trackKey(cache, key);

      backOff.eventually("The missing entry did not back-off.", ControlledExponentialBackOff.Event.BACK_OFF);
      assertTrue(iracManager.containsKey(key), "The key must be kept while the back-off is in progress");
      assertEquals(1, entryLoader.loadCount(key), "The entry must not be loaded again before the back-off ends");

      backOff.release();

      eventually("The key was never sent", () -> !iracManager.containsKey(key));
      assertEquals(2, entryLoader.loadCount(key), "The entry must be loaded again after the back-off");
   }

   /**
    * A failure to load the entry, for example, from the store, does not mean the entry is missing. The key must be
    * retried, but only after the backup's back-off, instead of immediately.
    */
   public void testLoadFailureWaitsForBackOff() {
      Cache<Object, Object> cache = cache(0, CACHE_WITH_STORE);
      IracManager iracManager = extractComponent(cache, IracManager.class);
      String key = getStringKeyForCache("load-failure", cache);
      cache.put(key, "value");
      eventually("The key was not sent", () -> !iracManager.containsKey(key));
      backOff.cleanupEvents();

      CountingEntryLoader entryLoader = wrapEntryLoader(cache);
      entryLoader.throwOnLoad(key, 1);

      trackKey(cache, key);

      backOff.eventually("The load failure did not back-off.", ControlledExponentialBackOff.Event.BACK_OFF);
      assertTrue(iracManager.containsKey(key), "The key must be kept while the back-off is in progress");
      assertEquals(1, entryLoader.loadCount(key), "The entry must not be loaded again before the back-off ends");

      backOff.release();

      eventually("The key was never sent", () -> !iracManager.containsKey(key));
      assertEquals(2, entryLoader.loadCount(key), "The entry must be loaded again after the back-off");
   }

   /**
    * A key whose entry never loads must not block the other keys. The failing key is kept to be retried, while the
    * keys loaded in the same round are still sent to the remote site.
    */
   public void testLoadFailureDoesNotBlockOtherKeys() {
      Cache<Object, Object> cache = cache(0, CACHE_WITH_STORE);
      IracManager iracManager = extractComponent(cache, IracManager.class);
      String failingKey = getStringKeyForCache("always-failing", cache);
      String healthyKey = getStringKeyForCache("healthy", cache);
      cache.put(failingKey, "value");
      cache.put(healthyKey, "value");
      eventually("The keys were not sent",
            () -> !iracManager.containsKey(failingKey) && !iracManager.containsKey(healthyKey));
      backOff.cleanupEvents();

      CountingEntryLoader entryLoader = wrapEntryLoader(cache);
      // every load fails, so the key is never sent while the test runs.
      entryLoader.throwOnLoad(failingKey, Integer.MAX_VALUE);

      trackKey(cache, failingKey);
      backOff.eventually("The load failure did not back-off.", ControlledExponentialBackOff.Event.BACK_OFF);

      // the back-off is in progress, so the healthy key waits for it to end. The round after the back-off has both keys.
      trackKey(cache, healthyKey);

      // A round started by trackKey() may still be running when the back-off ends. It enables the back-off again when
      // it completes, because the failing key is kept, so every back-off is released until the healthy key is sent.
      eventually("The healthy key was never sent", () -> {
         backOff.release();
         return !iracManager.containsKey(healthyKey);
      });
      assertThat(iracManager.containsKey(failingKey))
            .as("The key that fails to load must be kept to be retried")
            .isTrue();
   }

   /**
    * A batch with only missing entries has nothing to send, but it must not block the batches after it. Each batch
    * holds a single key, so the missing key's batch sends nothing, while the healthy keys are still sent in the same
    * round.
    */
   public void testMissingEntryDoesNotBlockOtherBatches() {
      Cache<Object, Object> cache = cache(0, CACHE_SINGLE_KEY_BATCH);
      IracManager iracManager = extractComponent(cache, IracManager.class);
      String missingKey = getStringKeyForCache("always-missing", cache);
      List<String> healthyKeys = new ArrayList<>(NUMBER_OF_HEALTHY_KEYS);
      for (int i = 0; i < NUMBER_OF_HEALTHY_KEYS; ++i) {
         healthyKeys.add(getStringKeyForCache("healthy-" + i, cache));
      }
      cache.put(missingKey, "value");
      healthyKeys.forEach(key -> cache.put(key, "value"));
      eventually("The keys were not sent",
            () -> !iracManager.containsKey(missingKey) && healthyKeys.stream().noneMatch(iracManager::containsKey));
      backOff.cleanupEvents();

      CountingEntryLoader entryLoader = wrapEntryLoader(cache);
      // the entry is in the store, but no load finds it while the test runs.
      entryLoader.failToLoad(missingKey, Integer.MAX_VALUE);

      trackKey(cache, missingKey);
      backOff.eventually("The missing entry did not back-off.", ControlledExponentialBackOff.Event.BACK_OFF);

      // the back-off is in progress, so the healthy keys wait for it to end. The round after the back-off has every key.
      // The keys are sent in the queue's order, and with several healthy keys, at least one comes after the missing key.
      healthyKeys.forEach(key -> trackKey(cache, key));

      // A round started by trackKey() may still be running when the back-off ends. It enables the back-off again when
      // it completes, because the missing key is kept, so every back-off is released until the healthy keys are sent.
      eventually("A healthy key was never sent", () -> {
         backOff.release();
         return healthyKeys.stream().noneMatch(iracManager::containsKey);
      });
      assertThat(iracManager.containsKey(missingKey))
            .as("The missing key must be kept to be retried")
            .isTrue();
   }

   /**
    * Reproducer for the busy retry loop: keys evicted from a cache without a store have neither an entry nor a
    * tombstone and they must not keep the IRAC executor busy forever.
    */
   public void testEvictedKeysAreDiscarded() {
      List<ManualIracManager> iracManagers = new ArrayList<>(2);
      for (Cache<Object, Object> cache : this.<Object, Object>caches(CACHE_EVICTION)) {
         ManualIracManager iracManager = ManualIracManager.wrapCache(cache);
         iracManager.enable();
         iracManagers.add(iracManager);
      }
      cleanupTasks.add(() -> iracManagers.forEach(m -> m.disable(ManualIracManager.DisableMode.DROP)));

      Cache<Object, Object> cache = cache(0, CACHE_EVICTION);
      for (int i = 0; i < NUMBER_OF_KEYS; ++i) {
         cache.put("key-" + i, "value-" + i);
      }

      // the test is only meaningful if keys were evicted before IRAC sends them. Both nodes own every key, so both
      // must have evicted. cleanUp() runs the pending evictions now, instead of waiting for them.
      for (Cache<Object, Object> c : this.<Object, Object>caches(CACHE_EVICTION)) {
         TestingUtil.cleanUpDataContainerForCache(c);
         InternalDataContainer<?, ?> dataContainer = extractComponent(c, InternalDataContainer.class);
         assertTrue(dataContainer.sizeIncludingExpired() < NUMBER_OF_KEYS, "No key was evicted from " + address(c));
      }

      // the keys are only sent now; most of them have been evicted in the meantime and, since the cache has no store,
      // they have neither an entry nor a tombstone.
      iracManagers.forEach(m -> m.disable(ManualIracManager.DisableMode.SEND));

      for (ManualIracManager iracManager : iracManagers) {
         eventually("The IRAC queue did not drain", iracManager::isEmpty, 30, TimeUnit.SECONDS);
      }
   }

   private void trackKey(Cache<?, ?> cache, Object key) {
      int segment = extractCacheTopology(cache).getSegment(key);
      extractComponent(cache, IracManager.class)
            .trackUpdatedKey(segment, key, RequestUUID.of(address(cache), requestId.incrementAndGet()));
   }

   private CountingEntryLoader wrapEntryLoader(Cache<?, ?> cache) {
      ClusteringDependentLogic cdl = extractComponent(cache, ClusteringDependentLogic.class);
      EntryLoader<Object, Object> original = cdl.getEntryLoader();
      CountingEntryLoader entryLoader = new CountingEntryLoader(original);
      replaceEntryLoader(cdl, entryLoader);
      cleanupTasks.add(() -> replaceEntryLoader(cdl, original));
      return entryLoader;
   }

   private static void replaceEntryLoader(ClusteringDependentLogic cdl, EntryLoader<Object, Object> entryLoader) {
      TestingUtil.replaceField(entryLoader, "entryLoader", cdl,
            ClusteringDependentLogic.AbstractClusteringDependentLogic.class);
   }

   private static ConfigurationBuilder configurationWithoutStore() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.clustering().hash().numOwners(2).numSegments(16);
      builder.sites().addBackup().site(REMOTE_SITE_NAME).strategy(BackupConfiguration.BackupStrategy.ASYNC);
      return builder;
   }

   private static ConfigurationBuilder configurationWithStore() {
      ConfigurationBuilder builder = configurationWithoutStore();
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return builder;
   }

   private static ConfigurationBuilder configurationWithWriteOnlyStore() {
      ConfigurationBuilder builder = configurationWithoutStore();
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).writeOnly(true);
      return builder;
   }

   private static ConfigurationBuilder configurationWithSingleKeyBatch() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.clustering().hash().numOwners(2).numSegments(16);
      // IRAC sends batches of the largest state transfer chunk size, so each batch has a single key.
      builder.sites().addBackup().site(REMOTE_SITE_NAME).strategy(BackupConfiguration.BackupStrategy.ASYNC)
            .stateTransfer().chunkSize(1);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return builder;
   }

   private static ConfigurationBuilder configurationWithEviction() {
      ConfigurationBuilder builder = configurationWithoutStore();
      builder.memory().maxCount(MAX_COUNT);
      return builder;
   }

   /**
    * An {@link EntryLoader} that counts the number of times a key is loaded and, optionally, simulates a key that is
    * not loadable yet.
    */
   private static class CountingEntryLoader implements EntryLoader<Object, Object> {

      private final EntryLoader<Object, Object> delegate;
      private final Map<Object, AtomicInteger> loadCount = new ConcurrentHashMap<>();
      private final Map<Object, Integer> failures = new ConcurrentHashMap<>();
      private final Map<Object, Integer> exceptions = new ConcurrentHashMap<>();

      CountingEntryLoader(EntryLoader<Object, Object> delegate) {
         this.delegate = delegate;
      }

      /**
       * The first {@code times} loads of the key do not find the entry.
       *
       * @param key   The key.
       * @param times The number of loads that do not find the entry.
       */
      void failToLoad(Object key, int times) {
         failures.put(key, times);
      }

      /**
       * The first {@code times} loads of the key complete exceptionally.
       *
       * @param key   The key.
       * @param times The number of loads that complete exceptionally.
       */
      void throwOnLoad(Object key, int times) {
         exceptions.put(key, times);
      }

      int loadCount(Object key) {
         AtomicInteger count = loadCount.get(key);
         return count == null ? 0 : count.get();
      }

      @Override
      public CompletionStage<InternalCacheEntry<Object, Object>> loadAndStoreInDataContainer(InvocationContext ctx,
            Object key, int segment, FlagAffectedCommand cmd) {
         int attempt = loadCount.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
         Integer exceptionTimes = exceptions.get(key);
         if (exceptionTimes != null && attempt <= exceptionTimes) {
            return CompletableFuture.failedFuture(new TestException("Induced load failure for key " + key));
         }
         Integer times = failures.get(key);
         if (times != null && attempt <= times) {
            return CompletableFutures.completedNull();
         }
         return delegate.loadAndStoreInDataContainer(ctx, key, segment, cmd);
      }
   }
}
