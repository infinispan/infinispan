package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.Exceptions;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.MissingMembersException;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.BlockingLocalTopologyManager.BlockedTopology;
import org.testng.SkipException;
import org.testng.annotations.Test;

@Test(testName = "globalstate.ThreeNodeTopologyReinstallTest", groups = "functional")
public class ThreeNodeTopologyReinstallTest extends AbstractGlobalStateRestartTest {

   private CacheMode cacheMode;

   private int getNumOwners() {
      return getClusterSize() - 1;
   }

   @Override
   protected int getClusterSize() {
      return 3;
   }

   @Override
   protected void applyCacheManagerClusteringConfiguration(ConfigurationBuilder config) {
      config.clustering().cacheMode(cacheMode).hash().numOwners(getNumOwners());
   }

   public void testReinstallTopologyByForce() throws Exception {
      executeTestRestart(true);
   }

   public void testReinstallTopologySafely() throws Exception {
      executeTestRestart(false, false);
   }

   /**
    * The coordinator sends the recovered topology and then starts the rebalance right away. Both messages are
    * asynchronous, so a recovering member can install the rebalance first and drop the recovered topology as late.
    * That member must still receive state for every segment it did not own in the recovered topology.
    */
   public void testReinstallTopologySafelyWhenRebalanceOvertakesStableTopology() throws Exception {
      if (cacheMode.isReplicated()) {
         // A replicated cache recovers with a new balanced topology, so no rebalance follows it.
         throw new SkipException("No rebalance after recovering a replicated cache");
      }
      executeTestRestart(false, true);
   }

   private void executeTestRestart(boolean force) throws Exception {
      executeTestRestart(force, false);
   }

   private void executeTestRestart(boolean force, boolean rebalanceFirst) throws Exception {
      boolean possibleDataLoss = !cacheMode.isReplicated() && force;
      var addressMappings = createInitialCluster();

      // Shutdown the cache cluster-wide
      cache(0, CACHE_NAME).shutdown();
      TestingUtil.killCacheManagers(this.cacheManagers);

      // Verify that the cache state file exists for all participants.
      for (int i = 0; i < getClusterSize(); i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertEquals(1, listFiles.length, Arrays.toString(listFiles));
      }
      this.cacheManagers.clear();

      // Partially recreate the cluster while trying to reinstall topology.
      int i = 0;
      for (; i < (getClusterSize() - getNumOwners()); i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
         TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));
         GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(manager(i));
         if (!cacheMode.isReplicated()) {
            Exceptions.expectException(MissingMembersException.class,
                  "ISPN000694: Cache 'testCache' has number of owners \\d but is missing too many members \\(\\d\\/3\\) to reinstall topology$",
                  () -> gcr.getClusterTopologyManager().useCurrentTopologyAsStable(CACHE_NAME, false));
         }
      }

      // Assert no rebalance for CACHE_NAME.
      assertThat(TestingUtil.extractGlobalComponentRegistry(manager(0))
            .getClusterTopologyManager()
            .isRebalancingEnabled(CACHE_NAME)).isFalse();

      // Since we didn't force the installation, operations still fail.
      assertOperationsFail();

      if (!force) {
         createStatefulCacheManager(Character.toString((char) ('A' + i++)), false);
         TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));
      }

      // Delay the recovered topology on one member until it installed the rebalance.
      int delayedNode = -1;
      BlockingLocalTopologyManager delayedTopologyManager = null;
      if (rebalanceFirst) {
         delayedNode = manager(0).isCoordinator() ? 1 : 0;
         delayedTopologyManager = BlockingLocalTopologyManager.replaceTopologyManager(manager(delayedNode), CACHE_NAME);
      }

      // Now we install the topology.
      for (int j = 0; j < cacheManagers.size(); j++) {
         EmbeddedCacheManager ecm = manager(j);
         if (ecm.isCoordinator()) {
            boolean stableTopology = TestingUtil.extractGlobalComponentRegistry(ecm)
                  .getClusterTopologyManager()
                  .useCurrentTopologyAsStable(CACHE_NAME, force);
            assertThat(stableTopology).isTrue();
            break;
         }
      }

      if (rebalanceFirst) {
         installRebalanceBeforeStableTopology(delayedTopologyManager);
      }
      waitForClusterToForm(CACHE_NAME);

      // Wait topology installation.
      AggregateCompletionStage<Void> topologyInstall = CompletionStages.aggregateCompletionStage();
      for (int j = 0; j < cacheManagers.size(); j++) {
         GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(manager(j));
         topologyInstall.dependsOn(gcr.getLocalTopologyManager().stableTopologyCompletion(CACHE_NAME));
      }
      CompletableFutures.uncheckedAwait(topologyInstall.freeze().toCompletableFuture(), 30, TimeUnit.SECONDS);

      if (possibleDataLoss) {
         // Varying the cache configuration and if forcing the topology, we can lose data.
         for (int j = 0; j < cacheManagers.size(); j++) {
            assertFalse(cache(j, CACHE_NAME).isEmpty());
            assertTrue(cache(j, CACHE_NAME).size() < DATA_SIZE);
         }
      } else {
         // In some cases, it is possible to recover all the data.
         checkData();
      }

      if (rebalanceFirst) {
         assertOwnerHoldsItsKeys(delayedNode);
      }

      // Now we add the missing members back.
      for (; i < getClusterSize(); i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }

      // This will create the cache, and trigger the join operation for the new managers.
      waitForClusterToForm(CACHE_NAME);
      checkClusterRestartedCorrectly(addressMappings);

      if (possibleDataLoss) {
         for (int j = 0; j < getClusterSize(); j++) {
            assertFalse(cache(j, CACHE_NAME).isEmpty());
         }
      } else {
         checkData();
      }
   }

   /**
    * Lets the rebalance start through before the recovered topology, whatever order they arrive in.
    * <p>
    * The recovered topology is released only after the rebalance was installed and its state transfer finished, so it
    * reaches the member as a late update. Blocking stops before the rebalance confirmation is released, so the
    * remaining rebalance phases are not held.
    *
    * @param topologyManager the blocking topology manager installed on the delayed member.
    * @throws InterruptedException if interrupted while waiting for a topology update or confirmation.
    */
   private static void installRebalanceBeforeStableTopology(BlockingLocalTopologyManager topologyManager)
         throws InterruptedException {
      BlockedTopology first = topologyManager.expectTopologyUpdate();
      BlockedTopology stable;
      BlockedTopology rebalance;
      if (first.getType() == BlockingLocalTopologyManager.Type.REBALANCE_START) {
         rebalance = first;
         stable = topologyManager.expectTopologyUpdate(CacheTopology.Phase.NO_REBALANCE);
      } else {
         stable = first;
         rebalance = topologyManager.expectTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL);
      }
      assertEquals(BlockingLocalTopologyManager.Type.CH_UPDATE, stable.getType());
      assertEquals(BlockingLocalTopologyManager.Type.REBALANCE_START, rebalance.getType());

      rebalance.unblock();
      // The member confirms the phase only after its state transfer completed.
      BlockingLocalTopologyManager.BlockedConfirmation confirmation = topologyManager.expectPhaseConfirmation();
      stable.unblock();
      topologyManager.stopBlocking();
      confirmation.unblock();
   }

   /**
    * Asserts the member holds every key it owns, reading only its own data container and store.
    *
    * @param node the index of the member to check.
    */
   private void assertOwnerHoldsItsKeys(int node) {
      Cache<Object, Object> cache = cache(node, CACHE_NAME);
      LocalizedCacheTopology topology = cache.getAdvancedCache().getDistributionManager().getCacheTopology();
      for (int v = 0; v < DATA_SIZE; v++) {
         String key = String.valueOf(v);
         if (topology.isReadOwner(key)) {
            assertEquals(key, cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).get(key),
                  "Owner " + address(node) + " is missing key " + key + " in segment " + topology.getSegment(key));
         }
      }
   }

   private void assertOperationsFail() {
      for (int i = 0; i < cacheManagers.size(); i++) {
         for (int v = 0; v < DATA_SIZE; v++) {
            final Cache<Object, Object> cache = cache(i, CACHE_NAME);
            String key = String.valueOf(v);
            // Always returns null. Message about not stable yet is logged.
            Exceptions.expectException(MissingMembersException.class,
                  "ISPN000689: Recovering cache 'testCache' but there are missing members, known members \\[.*\\] of a total of 3$",
                  () -> cache.get(key));
         }
      }
   }

   private ThreeNodeTopologyReinstallTest withCacheMode(CacheMode mode) {
      this.cacheMode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new ThreeNodeTopologyReinstallTest().withCacheMode(CacheMode.DIST_SYNC),
            new ThreeNodeTopologyReinstallTest().withCacheMode(CacheMode.REPL_SYNC),
      };
   }

   @Override
   protected String parameters() {
      return String.format("[cacheMode=%s]", cacheMode);
   }
}
