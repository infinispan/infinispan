package org.infinispan.eviction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Verifies a remove on a passivated cache still completes when the cache topology changes while the remove is
 * deleting the key from the store.
 * <p>
 * The topology change makes the remove retry in the new topology. The retried remove must complete, and the key must
 * not be readable from any node afterwards.
 * </p>
 */
@Test(groups = "functional", testName = "eviction.impl.PassivationRemoveTopologyChangeTest")
public class PassivationRemoveTopologyChangeTest extends MultipleCacheManagersTest {

   private static final String KEY = "k";
   private static final String VALUE = "v";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      cfg.persistence().passivation(true)
            .addStore(PassivationRemoveStoreOrderingTest.GatingStore.ConfigurationBuilder.class);
      createClusteredCaches(1, cfg);
   }

   public void testRemoveCompletesWhenTopologyChangesDuringStoreDelete() throws Exception {
      AdvancedCache<String, String> originator = this.<String, String>cache(0).getAdvancedCache();
      originator.put(KEY, VALUE);

      PassivationRemoveStoreOrderingTest.GatingStore store = TestingUtil.getFirstStore(originator);
      int initialTopologyId = currentTopologyId(originator);

      // Park the remove while it deletes the key from the store, before the entry is committed.
      Future<String> remove = fork(() -> originator.remove(KEY));
      store.deleteEntered.get(10, TimeUnit.SECONDS);

      // A node joining installs a new topology, so the remove fails its topology check and retries.
      // The joiner uses a plain store so the retry is not gated on it.
      ConfigurationBuilder joinerCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      joinerCfg.persistence().passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      EmbeddedCacheManager joiner = addClusterEnabledCacheManager(joinerCfg);
      Future<Cache<String, String>> joined = fork(() -> joiner.getCache());
      eventually(() -> currentTopologyId(originator) > initialTopologyId);

      store.releaseDelete();

      // The retry reads the previous value from the state left by the first attempt, which already deleted the key from
      // the originator's store and container. The return value depends on which node executes the retry: null on the
      // originator, or the value the joiner received through state transfer. Only completion is asserted.
      remove.get(10, TimeUnit.SECONDS);
      Cache<String, String> joinerCache = joined.get(10, TimeUnit.SECONDS);

      assertThat(originator.get(KEY)).as("removed entry readable on the originator").isNull();
      assertThat(joinerCache.get(KEY)).as("removed entry readable on the joiner").isNull();
   }

   private static int currentTopologyId(AdvancedCache<?, ?> cache) {
      return cache.getDistributionManager().getCacheTopology().getTopologyId();
   }
}
