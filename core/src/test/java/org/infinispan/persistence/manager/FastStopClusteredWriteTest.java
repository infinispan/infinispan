package org.infinispan.persistence.manager;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getFirstStore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManagerLockLeakTest.ControllableStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies that a write in progress on a node whose store does not answer neither blocks that node's cache stop nor
 * fails the write for the originator.
 *
 * <p>
 * Every store is purged on startup, so the stopping node fails its pending store operations instead of waiting for
 * them. The originator treats the failure as the node leaving, and the write completes on the remaining owner.
 * </p>
 *
 * @since 16.3
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "persistence.manager.FastStopClusteredWriteTest")
public class FastStopClusteredWriteTest extends MultipleCacheManagersTest {

   private static final int NUM_KEYS = 10;
   // Guard only: the stop and the write finish without waiting for the store.
   private static final int TIMEOUT_SECONDS = 10;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      // Both nodes own every key, so node 1 writes all of them to its store, as primary or backup.
      builder.clustering().hash().numOwners(2);
      builder.persistence().addStore(ControllableStore.ConfigurationBuilder.class).purgeOnStartup(true);
      createClusteredCaches(2, builder);
   }

   public void testStopNodeWithPendingWrite() throws Exception {
      // ControllableStore is a store of <Object, Object>, so the caches use the same types.
      Cache<Object, Object> originator = cache(0);
      Cache<Object, Object> stopping = cache(1);
      PersistenceManagerImpl stoppingPersistence = (PersistenceManagerImpl) extractComponent(stopping, PersistenceManager.class);
      ControllableStore stoppingStore = getFirstStore(stopping);

      Map<String, String> entries = new HashMap<>();
      for (int i = 0; i < NUM_KEYS; i++) {
         entries.put("k" + i, "v" + i);
      }

      try {
         // Node 1's store never answers on its own.
         stoppingStore.delayNextOperation();
         CompletableFuture<Void> putAll = originator.putAllAsync(entries);
         eventually(stoppingPersistence::anyLocksHeld);

         fork(() -> stopping.stop()).get(TIMEOUT_SECONDS, SECONDS);

         putAll.get(TIMEOUT_SECONDS, SECONDS);
         assertThat(originator.getAdvancedCache().getAll(entries.keySet())).isEqualTo(entries);
      } finally {
         // Lets a stop still waiting on the store finish, so the cache managers can stop.
         stoppingStore.endDelay();
      }
   }
}
