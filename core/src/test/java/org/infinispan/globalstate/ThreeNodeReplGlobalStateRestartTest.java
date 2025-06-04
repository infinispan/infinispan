package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.topology.LocalTopologyManager;
import org.testng.annotations.Test;

@Test(testName = "globalstate.ThreeNodeReplGlobalStateRestartTest", groups = "functional")
public class ThreeNodeReplGlobalStateRestartTest extends AbstractGlobalStateRestartTest {

   @Override
   protected int getClusterSize() {
      return 3;
   }

   @Override
   protected void applyCacheManagerClusteringConfiguration(ConfigurationBuilder config) {
      config.clustering().cacheMode(CacheMode.REPL_SYNC);
   }

   public void testGracefulShutdownAndRestart() throws Throwable {
      shutdownAndRestart(-1, false);
   }

   public void testGracefulShutdownAndRestartReverseOrder() throws Throwable {
      shutdownAndRestart(-1, true);
   }

   public void testFailedRestartWithExtraneousCoordinator() throws Throwable {
      shutdownAndRestart(0, false);
   }

   public void testFailedRestartWithExtraneousNode() throws Throwable {
      shutdownAndRestart(1, false);
   }

   public void testAddMemberAfterRecover() throws Throwable {
      shutdownAndRestart(-1, false);
      createStatefulCacheManager(Character.toString('@'), true);
      waitForClusterToForm(CACHE_NAME);

      // Ensure that the cache contains the right data.
      // Use the extraneous member to check.
      int index = getClusterSize();
      assertEquals(DATA_SIZE, cache(index, CACHE_NAME).size());
      for (int i = 0; i < DATA_SIZE; i++) {
         assertEquals(cache(index, CACHE_NAME).get(String.valueOf(i)), String.valueOf(i));
      }
   }

   public void testDisableRebalanceRestartEnableRebalance() throws Throwable {
      var addressMappings = createInitialCluster();
      ConsistentHash oldConsistentHash = advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();

      GlobalComponentRegistry.of(manager(0)).getLocalTopologyManager().setRebalancingEnabled(false);

      for (int i = 0; i < getClusterSize(); i++) {
         ((DefaultCacheManager) manager(i)).shutdownAllCaches();
         manager(i).stop();
      }

      cacheManagers.clear();

      createStatefulCacheManagers(false, -1, false);

      for (int i = 0; i < getClusterSize() - 1; i++) {
         cache(i, CACHE_NAME);
      }

      LocalTopologyManager ltm = GlobalComponentRegistry.of(manager(0)).getLocalTopologyManager();
      assertThat(ltm.isRebalancingEnabled()).isFalse();

      ltm.setRebalancingEnabled(true);

      // Last missing.
      cache(getClusterSize() - 1, CACHE_NAME);

      assertHealthyCluster(addressMappings, oldConsistentHash);
      assertTrue(GlobalComponentRegistry.of(manager(0)).getLocalTopologyManager().isRebalancingEnabled());
   }

   public void testPersistentStateIsDeletedAfterRestart() throws Throwable {
      shutdownAndRestart(-1, false);

      restartWithoutGracefulShutdown();
   }
}
