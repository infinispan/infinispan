package org.infinispan.xsite.irac;

import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests for keys tracked by {@link IracManager} whose entry and tombstone are both missing.
 * <p>
 * The entry can be temporarily missing while passivation is in progress after an eviction and, in that case, the key
 * must be retried. However, the key must not be retried forever when the entry can never be loaded, for example, when
 * it is evicted from a cache without a store.
 *
 * @since 16.3
 */
@Test(groups = "xsite", testName = "xsite.irac.IracMissingEntryTest")
public class IracMissingEntryTest extends MultipleCacheManagersTest {

   private static final String SITE_NAME = "LON-1";
   private static final String REMOTE_SITE_NAME = "NYC";
   private static final String CACHE_NO_STORE = "irac-no-store";
   private static final String CACHE_WITH_STORE = "irac-with-store";
   private static final String CACHE_EVICTION = "irac-eviction";
   private static final int MAX_COUNT = 10;
   private static final int NUMBER_OF_KEYS = 50;
   // must be kept in sync with DefaultIracManager.MAX_MISSING_ENTRY_ATTEMPTS
   private static final int MAX_MISSING_ENTRY_ATTEMPTS = 10;

   private final AtomicLong requestId = new AtomicLong();
   private final List<Runnable> cleanupTasks = new ArrayList<>(2);

   @Override
   protected void createCacheManagers() throws Throwable {
      TransportFlags flags = new TransportFlags().withSiteIndex(0).withSiteName(SITE_NAME).withFD(true);
      createClusteredCaches(2, CACHE_NO_STORE, configurationWithoutStore(), flags);
      defineConfigurationOnAllManagers(CACHE_WITH_STORE, configurationWithStore());
      waitForClusterToForm(CACHE_WITH_STORE);
      defineConfigurationOnAllManagers(CACHE_EVICTION, configurationWithEviction());
      waitForClusterToForm(CACHE_EVICTION);
      for (String cacheName : List.of(CACHE_NO_STORE, CACHE_WITH_STORE, CACHE_EVICTION)) {
         for (Cache<?, ?> cache : caches(cacheName)) {
            // nothing is sent to the remote site; the keys are removed from the queue as if they were sent.
            extractComponent(cache, TakeOfflineManager.class).takeSiteOffline(REMOTE_SITE_NAME);
         }
      }
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      cleanupTasks.forEach(Runnable::run);
      cleanupTasks.clear();
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
    * The entry may not be loadable while passivation is in progress after an eviction. The key must be retried until
    * the entry is available.
    */
   public void testMissingEntryWithStoreIsRetried() {
      Cache<Object, Object> cache = cache(0, CACHE_WITH_STORE);
      IracManager iracManager = extractComponent(cache, IracManager.class);
      String key = getStringKeyForCache("missing-with-store", cache);
      cache.put(key, "value");
      eventually("The key was not sent", () -> !iracManager.containsKey(key));

      CountingEntryLoader entryLoader = wrapEntryLoader(cache);
      entryLoader.failToLoad(key, 3);

      trackKey(cache, key);

      eventually("The key was never sent", () -> !iracManager.containsKey(key));
      assertEquals(4, entryLoader.loadCount(key), "The entry must be retried until it is loaded");
   }

   /**
    * If the entry is never loadable, the key must be discarded after a bounded number of attempts.
    */
   public void testMissingEntryWithStoreIsDiscardedAfterMaxAttempts() {
      Cache<Object, Object> cache = cache(0, CACHE_WITH_STORE);
      IracManager iracManager = extractComponent(cache, IracManager.class);
      // the key is never written, so it is never loadable.
      String key = getStringKeyForCache("never-loadable", cache);
      CountingEntryLoader entryLoader = wrapEntryLoader(cache);

      trackKey(cache, key);

      eventually("The key was never discarded", () -> !iracManager.containsKey(key), 30, TimeUnit.SECONDS);
      int loadCount = entryLoader.loadCount(key);
      assertTrue(loadCount > 1, "The entry must be retried, loaded " + loadCount + " time(s)");
      assertTrue(loadCount <= MAX_MISSING_ENTRY_ATTEMPTS + 1,
            "The entry must not be retried forever, loaded " + loadCount + " time(s)");
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

      CountingEntryLoader(EntryLoader<Object, Object> delegate) {
         this.delegate = delegate;
      }

      void failToLoad(Object key, int times) {
         failures.put(key, times);
      }

      int loadCount(Object key) {
         AtomicInteger count = loadCount.get(key);
         return count == null ? 0 : count.get();
      }

      @Override
      public CompletionStage<InternalCacheEntry<Object, Object>> loadAndStoreInDataContainer(InvocationContext ctx,
            Object key, int segment, FlagAffectedCommand cmd) {
         int attempt = loadCount.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
         Integer times = failures.get(key);
         if (times != null && attempt <= times) {
            return CompletableFutures.completedNull();
         }
         return delegate.loadAndStoreInDataContainer(ctx, key, segment, cmd);
      }
   }
}
