package org.infinispan.eviction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Verifies a remove, or a compute, merge or functional write that removes, on a cache with a store notifies listeners
 * with the removed value and the removed entry's metadata.
 * <p>
 * Covers entries resident in memory and entries only in the store, with and without passivation.
 * </p>
 */
@Test(groups = "functional", testName = "eviction.impl.RemovedEventPreviousValueTest")
public class RemovedEventPreviousValueTest extends SingleCacheManagerTest {

   private static final long LIFESPAN = TimeUnit.HOURS.toMillis(1);

   private boolean passivation;

   public RemovedEventPreviousValueTest withPassivation(boolean passivation) {
      this.passivation = passivation;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new RemovedEventPreviousValueTest().withPassivation(true),
            new RemovedEventPreviousValueTest().withPassivation(false)
      };
   }

   @Override
   protected String parameters() {
      return "[passivation=" + passivation + "]";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence().passivation(passivation)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getSimpleName() + parameters());
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testRemoveReturnsPreviousValueOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("stored-return", "value");
      // Leaves the entry only in the store.
      testCache.evict("stored-return");

      assertThat(testCache.remove("stored-return")).isEqualTo("value");
      assertThat(testCache.get("stored-return")).isNull();
   }

   public void testRemovedEventCarriesPreviousValueOfResidentEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("resident", "value");

      assertThat(removedValuesAfter(testCache, c -> assertThat(c.remove("resident")).isEqualTo("value")))
            .containsExactly("value");
   }

   public void testRemovedEventCarriesPreviousValueOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("stored", "value");
      // Leaves the entry only in the store.
      testCache.evict("stored");

      assertThat(removedValuesAfter(testCache, c -> assertThat(c.remove("stored")).isEqualTo("value")))
            .containsExactly("value");
   }

   public void testComputeRemovedEventCarriesPreviousValueOfResidentEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("compute-resident", "value");

      assertThat(removedValuesAfter(testCache, c -> c.compute("compute-resident", (k, v) -> null)))
            .containsExactly("value");
   }

   public void testComputeRemovedEventCarriesPreviousValueOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("compute-stored", "value");
      // Leaves the entry only in the store.
      testCache.evict("compute-stored");

      assertThat(removedValuesAfter(testCache, c -> c.compute("compute-stored", (k, v) -> null)))
            .containsExactly("value");
   }

   public void testMergeRemovedEventCarriesPreviousValueOfResidentEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("merge-resident", "value");

      assertThat(removedValuesAfter(testCache, c -> c.merge("merge-resident", "other", (oldValue, value) -> null)))
            .containsExactly("value");
   }

   public void testMergeRemovedEventCarriesPreviousValueOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("merge-stored", "value");
      // Leaves the entry only in the store.
      testCache.evict("merge-stored");

      assertThat(removedValuesAfter(testCache, c -> c.merge("merge-stored", "other", (oldValue, value) -> null)))
            .containsExactly("value");
   }

   public void testFunctionalRemovedEventCarriesPreviousValueOfResidentEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("functional-resident", "value");

      assertThat(removedValuesAfter(testCache, c -> functionalRemove(c, "functional-resident")))
            .containsExactly("value");
   }

   public void testFunctionalRemovedEventCarriesPreviousValueOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("functional-stored", "value");
      // Leaves the entry only in the store.
      testCache.evict("functional-stored");

      assertThat(removedValuesAfter(testCache, c -> functionalRemove(c, "functional-stored")))
            .containsExactly("value");
   }

   public void testRemovedEventCarriesMetadataOfResidentEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      putWithLifespan(testCache, "metadata-resident", false);

      assertThat(removedLifespansAfter(testCache, c -> c.remove("metadata-resident")))
            .containsExactly(LIFESPAN);
   }

   public void testRemovedEventCarriesMetadataOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      putWithLifespan(testCache, "metadata-stored", true);

      assertThat(removedLifespansAfter(testCache, c -> c.remove("metadata-stored")))
            .containsExactly(LIFESPAN);
   }

   public void testComputeRemovedEventCarriesMetadataOfResidentEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      putWithLifespan(testCache, "metadata-compute-resident", false);

      assertThat(removedLifespansAfter(testCache, c -> c.compute("metadata-compute-resident", (k, v) -> null)))
            .containsExactly(LIFESPAN);
   }

   public void testComputeRemovedEventCarriesMetadataOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      putWithLifespan(testCache, "metadata-compute-stored", true);

      assertThat(removedLifespansAfter(testCache, c -> c.compute("metadata-compute-stored", (k, v) -> null)))
            .containsExactly(LIFESPAN);
   }

   public void testMergeRemovedEventCarriesMetadataOfResidentEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      putWithLifespan(testCache, "metadata-merge-resident", false);

      assertThat(removedLifespansAfter(testCache,
            c -> c.merge("metadata-merge-resident", "other", (oldValue, value) -> null)))
            .containsExactly(LIFESPAN);
   }

   public void testMergeRemovedEventCarriesMetadataOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      putWithLifespan(testCache, "metadata-merge-stored", true);

      assertThat(removedLifespansAfter(testCache,
            c -> c.merge("metadata-merge-stored", "other", (oldValue, value) -> null)))
            .containsExactly(LIFESPAN);
   }

   public void testFunctionalRemovedEventCarriesMetadataOfResidentEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      putWithLifespan(testCache, "metadata-functional-resident", false);

      assertThat(removedLifespansAfter(testCache, c -> functionalRemove(c, "metadata-functional-resident")))
            .containsExactly(LIFESPAN);
   }

   public void testFunctionalRemovedEventCarriesMetadataOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      putWithLifespan(testCache, "metadata-functional-stored", true);

      assertThat(removedLifespansAfter(testCache, c -> functionalRemove(c, "metadata-functional-stored")))
            .containsExactly(LIFESPAN);
   }

   /**
    * Writes {@code "value"} under {@code key} with a {@link #LIFESPAN} lifespan, so the entry's metadata differs from
    * the default metadata a removal could carry.
    *
    * @param testCache the cache to write to.
    * @param key       the key to write.
    * @param stored    {@code true} to leave the entry only in the store, {@code false} to keep it resident in memory.
    */
   private static void putWithLifespan(Cache<String, String> testCache, String key, boolean stored) {
      // The lifespan is far beyond the test duration: it only marks the metadata, the entry never expires.
      testCache.put(key, "value", LIFESPAN, TimeUnit.MILLISECONDS);
      if (stored)
         testCache.evict(key);
   }

   /**
    * Removes {@code key} through the functional read-write API and asserts it returned the removed value.
    *
    * @param testCache the cache to remove from.
    * @param key       the key holding {@code "value"}.
    */
   private static void functionalRemove(Cache<String, String> testCache, String key) {
      String previous = FunctionalMap.create(testCache.getAdvancedCache()).toReadWriteMap()
            .eval(key, MarshallableFunctions.<String, String>removeReturnPrevOrNull())
            .join();
      assertThat(previous).isEqualTo("value");
   }

   /**
    * Runs {@code removal} with a post-commit listener registered and returns the previous values it observed.
    *
    * @param testCache the cache to remove from.
    * @param removal   the operation that removes an entry holding {@code "value"}.
    * @return the old values carried by the {@code CacheEntryRemoved} events, in notification order.
    */
   private static List<String> removedValuesAfter(Cache<String, String> testCache, Consumer<Cache<String, String>> removal) {
      return listenDuring(testCache, removal).previousValues;
   }

   /**
    * Runs {@code removal} with a post-commit listener registered and returns the lifespans of the metadata it observed.
    *
    * @param testCache the cache to remove from.
    * @param removal   the operation that removes an entry written with {@link #putWithLifespan}.
    * @return the lifespans carried by the {@code CacheEntryRemoved} events' metadata, in notification order, with
    *         {@code null} for an event without metadata.
    */
   private static List<Long> removedLifespansAfter(Cache<String, String> testCache, Consumer<Cache<String, String>> removal) {
      return listenDuring(testCache, removal).lifespans;
   }

   /**
    * Runs {@code removal} with a {@link RemovedListener} registered, and unregisters it afterwards.
    *
    * @param testCache the cache to remove from.
    * @param removal   the operation that removes an entry.
    * @return the listener, holding what the {@code CacheEntryRemoved} events carried.
    */
   private static RemovedListener listenDuring(Cache<String, String> testCache, Consumer<Cache<String, String>> removal) {
      RemovedListener listener = new RemovedListener();
      testCache.addListener(listener);
      try {
         removal.accept(testCache);
      } finally {
         testCache.removeListener(listener);
      }
      return listener;
   }

   @Listener(observation = Listener.Observation.POST)
   public static class RemovedListener {
      final List<String> previousValues = new CopyOnWriteArrayList<>();
      // CopyOnWriteArrayList accepts null, recorded when an event carries no metadata.
      final List<Long> lifespans = new CopyOnWriteArrayList<>();

      @CacheEntryRemoved
      public void onRemoved(CacheEntryRemovedEvent<String, String> event) {
         previousValues.add(event.getOldValue());
         Metadata metadata = event.getMetadata();
         lifespans.add(metadata == null ? null : metadata.lifespan());
      }
   }
}
