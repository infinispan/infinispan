package org.infinispan.eviction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Verifies a remove on a cache with a store returns the removed value and notifies listeners with it.
 * <p>
 * Covers entries resident in memory and entries only in the store, with and without passivation.
 * </p>
 */
@Test(groups = "functional", testName = "eviction.impl.RemovedEventPreviousValueTest")
public class RemovedEventPreviousValueTest extends SingleCacheManagerTest {

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

      assertThat(removedValuesAfterRemoving(testCache, "resident")).containsExactly("value");
   }

   public void testRemovedEventCarriesPreviousValueOfStoredEntry() {
      Cache<String, String> testCache = cacheManager.getCache();
      testCache.put("stored", "value");
      // Leaves the entry only in the store.
      testCache.evict("stored");

      assertThat(removedValuesAfterRemoving(testCache, "stored")).containsExactly("value");
   }

   /**
    * Removes {@code key} and returns the previous values the listener observed after the remove committed.
    */
   private static List<String> removedValuesAfterRemoving(Cache<String, String> testCache, String key) {
      RemovedListener listener = new RemovedListener();
      testCache.addListener(listener);
      try {
         assertThat(testCache.remove(key)).isEqualTo("value");
      } finally {
         testCache.removeListener(listener);
      }
      return listener.previousValues;
   }

   @Listener(observation = Listener.Observation.POST)
   public static class RemovedListener {
      final List<String> previousValues = new CopyOnWriteArrayList<>();

      @CacheEntryRemoved
      public void onRemoved(CacheEntryRemovedEvent<String, String> event) {
         previousValues.add(event.getOldValue());
      }
   }
}
