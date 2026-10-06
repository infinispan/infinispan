package org.infinispan.eviction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Verifies a remove on a passivated cache still completes when the cache topology changes while the remove is
 * deleting the key from the store.
 * <p>
 * The topology change makes the remove retry in the new topology. The retried remove must complete, and the key must
 * not be readable from any node afterwards, including a joiner that received the key through state transfer before the
 * remove was retried. The originator of the remove ends up as either the primary or the backup owner of the key in the
 * new topology.
 * </p>
 */
@Test(groups = "functional", testName = "eviction.impl.PassivationRemoveTopologyChangeTest")
public class PassivationRemoveTopologyChangeTest extends MultipleCacheManagersTest {

   private static final String KEY = "k";
   private static final String VALUE = "v";

   private boolean originatorPrimary;
   private ControlledConsistentHashFactory<?> chFactory;

   public PassivationRemoveTopologyChangeTest() {
      // Each method starts from a single node, with a fresh store gate.
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new PassivationRemoveTopologyChangeTest().originatorPrimary(true),
            new PassivationRemoveTopologyChangeTest().originatorPrimary(false),
      };
   }

   PassivationRemoveTopologyChangeTest originatorPrimary(boolean originatorPrimary) {
      this.originatorPrimary = originatorPrimary;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"originatorPrimary"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{originatorPrimary};
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      // A single segment owned by both nodes once the joiner is in. Index 0 is the originator and index 1 the joiner.
      // While the originator is alone the factory drops the joiner's index, so the originator owns the key either way.
      chFactory = originatorPrimary
            ? new ControlledConsistentHashFactory.Default(0, 1)
            : new ControlledConsistentHashFactory.Default(1, 0);
      createClusteredCaches(1, ControlledConsistentHashFactory.SCI.INSTANCE,
            passivatedConfig(PassivationRemoveStoreOrderingTest.GatingStore.ConfigurationBuilder.class));
   }

   public void testRemoveCompletesWhenTopologyChangesDuringStoreDelete() throws Exception {
      AdvancedCache<String, String> originator = this.<String, String>cache(0).getAdvancedCache();
      originator.put(KEY, VALUE);

      Cache<String, String> joinerCache = removeWhileNodeJoins(originator);

      assertThat(originator.get(KEY)).as("removed entry readable on the originator").isNull();
      assertThat(joinerCache.get(KEY)).as("removed entry readable on the joiner").isNull();
   }

   public void testRemoveOfStoredEntryCompletesWhenTopologyChangesDuringStoreDelete() throws Exception {
      AdvancedCache<String, String> originator = this.<String, String>cache(0).getAdvancedCache();
      originator.put(KEY, VALUE);
      // Passivate the key, so it only exists in the store when the remove starts.
      originator.evict(KEY);
      assertThat(originator.getDataContainer().containsKey(KEY)).as("key resident before the remove").isFalse();

      Cache<String, String> joinerCache = removeWhileNodeJoins(originator);

      assertThat(originator.get(KEY)).as("removed entry readable on the originator").isNull();
      assertThat(joinerCache.get(KEY)).as("removed entry readable on the joiner").isNull();
   }

   /**
    * Removes {@link #KEY} from {@code originator} while a new node joins the cluster.
    * <p>
    * The remove is parked while it deletes the key from the store, so the joiner receives the key through state
    * transfer before the remove resumes. The join changes the topology, so the remove then retries.
    * </p>
    *
    * @param originator the cache that removes the key.
    * @return the joiner's cache, once the remove completes.
    */
   private Cache<String, String> removeWhileNodeJoins(AdvancedCache<String, String> originator) throws Exception {
      PassivationRemoveStoreOrderingTest.GatingStore store = TestingUtil.getFirstStore(originator);

      // Park the remove while it deletes the key from the store, before the entry is committed.
      Future<String> remove = fork(() -> originator.remove(KEY));
      store.deleteEntered.get(10, TimeUnit.SECONDS);

      // A node joining installs a new topology, so the remove fails its topology check and retries.
      // The joiner uses a plain store so the retry is not gated on it.
      EmbeddedCacheManager joiner = addClusterEnabledCacheManager(ControlledConsistentHashFactory.SCI.INSTANCE,
            passivatedConfig(DummyInMemoryStoreConfigurationBuilder.class));

      // Getting the cache waits for the joiner's state transfer. The parked remove has not deleted the key from the
      // originator's store or container yet, so the joiner receives the key before the remove resumes.
      Cache<String, String> joinerCache = joiner.getCache();
      TestingUtil.waitForNoRebalance(originator, joinerCache);
      // Local read, so it does not reach the originator, which is parked holding the key's lock.
      assertThat(joinerCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).get(KEY))
            .as("joiner received the key through state transfer").isEqualTo(VALUE);

      store.releaseDelete();

      // The first attempt already deleted the key from the originator's store and container, so the return value
      // depends on which node runs the retry: null on the originator, or the value the joiner received through state
      // transfer. Only completion is asserted.
      remove.get(10, TimeUnit.SECONDS);
      return joinerCache;
   }

   private ConfigurationBuilder passivatedConfig(Class<? extends StoreConfigurationBuilder<?, ?>> store) {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      cfg.clustering().hash().numSegments(1).numOwners(2);
      cfg.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(chFactory);
      cfg.persistence().passivation(true).addStore(store);
      return cfg;
   }
}
