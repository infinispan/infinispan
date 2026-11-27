package org.infinispan.eviction.impl;

import static org.infinispan.test.TestingUtil.getFirstStore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PassivationPersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.support.FailStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests that an entry is not silently lost when the cache store write fails while the entry is being passivated after
 * eviction (ISPN-18113).
 * <p>
 * The first test method is the reproducer originally proposed in PR #17905: it drives the organic {@code maxCount}
 * size-based eviction path and asserts that no entry is lost when a passivation write fails. The remaining methods use
 * manual eviction to deterministically exercise the edge cases around the retained entry: it must not be resurrected
 * after an explicit removal or clear, and it must be flushed to the store once the store recovers (on a later
 * passivation of the key or on shutdown).
 * </p>
 */
@Test(groups = "functional", testName = "eviction.impl.PassivationFailureDataLossTest")
public class PassivationFailureDataLossTest extends SingleCacheManagerTest {

   private static final int MAX_ENTRIES = 2;

   public PassivationFailureDataLossTest() {
      // Recreate the cache manager for every method so the simulated store failures of one test do not leak into another.
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.memory().maxCount(MAX_ENTRIES);
      cfg.persistence()
            .passivation(true)
            .addStore(FailStore.ConfigurationBuilder.class);
      return TestCacheManagerFactory.createCacheManager(new GlobalConfigurationBuilder().nonClusteredDefault(), cfg);
   }

   public void testEntryNotLostWhenPassivationFails() {
      cache.put("k1", "v1");
      cache.put("k2", "v2");

      FailStore fs = getFirstStore(cache);
      fs.failModification(1);  // next passivation write throws TestException

      // maxCount will trigger the passivation
      cache.put("k3", "v3");

      eventually(() -> cache.getAdvancedCache().getDataContainer().size() <= MAX_ENTRIES);

      assertNotNull(cache.get("k1"), "cannot recover k1 entry");
      assertNotNull(cache.get("k2"), "cannot recover k2 entry");
      assertNotNull(cache.get("k3"), "cannot recover k3 entry");
   }

   public void testEvictedEntryNotLostWhenStoreWriteFails() {
      cache.put("k1", "v1");

      failNextModification();
      cache.evict("k1");

      // The write failed, so the entry is neither in the data container nor in the store...
      assertFalse(cache.getAdvancedCache().getDataContainer().containsKey("k1"));
      assertFalse(store().contains(storageKey("k1")));
      // ...but it has been retained in memory instead of being silently dropped.
      assertEquals(1, passivationPersistenceManager().pendingPassivations());
      assertEquals("v1", cache.get("k1"));
   }

   public void testRemoveOfQuarantinedEntryIsNotResurrected() {
      cache.put("k1", "v1");

      failNextModification();
      cache.evict("k1");
      assertEquals(1, passivationPersistenceManager().pendingPassivations());

      assertEquals("v1", cache.remove("k1"));

      assertEquals(0, passivationPersistenceManager().pendingPassivations());
      assertFalse(cache.containsKey("k1"));
      assertNull(cache.get("k1"));
   }

   public void testClearDiscardsQuarantinedEntries() {
      cache.put("k1", "v1");

      failNextModification();
      cache.evict("k1");
      assertEquals(1, passivationPersistenceManager().pendingPassivations());

      cache.clear();

      assertEquals(0, passivationPersistenceManager().pendingPassivations());
      assertNull(cache.get("k1"));
   }

   public void testQuarantinedEntryPersistedWhenStoreRecoversAndReEvicted() {
      cache.put("k1", "v1");

      failNextModification();
      cache.evict("k1");
      assertEquals(1, passivationPersistenceManager().pendingPassivations());
      // Entry is still readable from memory while the store is down.
      assertEquals("v1", cache.get("k1"));

      // The store has recovered (the single simulated failure is consumed), so passivating the key again succeeds.
      cache.evict("k1");

      assertEquals(0, passivationPersistenceManager().pendingPassivations());
      assertTrue(store().contains(storageKey("k1")));
      assertEquals("v1", cache.get("k1"));
   }

   public void testPendingEntriesFlushedToStoreOnPassivateAll() {
      cache.put("k1", "v1");

      failNextModification();
      cache.evict("k1");
      assertEquals(1, passivationPersistenceManager().pendingPassivations());

      // The store has recovered before shutdown; passivateAll must flush the retained entry.
      CompletionStages.join(TestingUtil.extractComponent(cache, PassivationManager.class).passivateAllAsync());

      assertEquals(0, passivationPersistenceManager().pendingPassivations());
      assertTrue(store().contains(storageKey("k1")));
   }

   private void failNextModification() {
      store().failModification(1);
   }

   private FailStore store() {
      return getFirstStore(cache);
   }

   private PassivationPersistenceManager passivationPersistenceManager() {
      return (PassivationPersistenceManager) TestingUtil.extractComponent(cache, PersistenceManager.class);
   }

   private Object storageKey(String key) {
      return cache.getAdvancedCache().getKeyDataConversion().toStorage(key);
   }
}
