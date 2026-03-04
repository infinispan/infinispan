package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.Exceptions;
import org.infinispan.topology.MissingMembersException;
import org.testng.annotations.Test;

@Test(testName = "globalstate.FourNodeTopologyReinstallTest", groups = "functional")
public class FourNodeTopologyReinstallTest extends AbstractGlobalStateRestartTest {

   private static final int NUM_OWNERS = 2;

   @Override
   protected int getClusterSize() {
      return 4;
   }

   @Override
   protected void applyCacheManagerClusteringConfiguration(ConfigurationBuilder config) {
      config.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(NUM_OWNERS);
   }

   public void testSafetyCheckWithNodeChurn() {
      createInitialCluster();
      cache(0, CACHE_NAME).shutdown();
      TestingUtil.killCacheManagers(this.cacheManagers);
      assertStateFiles();
      this.cacheManagers.clear();

      // Start A and B: 2 present, 2 missing >= 2 owners, reject installing topolog.y
      createStatefulCacheManager("A", false);
      createStatefulCacheManager("B", false);
      TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));

      GlobalComponentRegistry gcrA = TestingUtil.extractGlobalComponentRegistry(manager(0));
      Exceptions.expectException(MissingMembersException.class,
            "ISPN000694.*missing too many members.*",
            () -> gcrA.getClusterTopologyManager().useCurrentTopologyAsStable(CACHE_NAME, false));

      // Stops node B before recovery.
      EmbeddedCacheManager bManager = cacheManagers.remove(1);
      TestingUtil.killCacheManagers(bManager);
      TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));

      // Start C. Cluster has {A, C}, still 2 missing (B, D) >= 2 owners. Should still reject installing topology.
      createStatefulCacheManager("C", false);
      TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));

      GlobalComponentRegistry gcrAfterChurn = TestingUtil.extractGlobalComponentRegistry(manager(0));
      Exceptions.expectException(MissingMembersException.class,
            "ISPN000694.*missing too many members.*",
            () -> gcrAfterChurn.getClusterTopologyManager().useCurrentTopologyAsStable(CACHE_NAME, false));

      // Create the last node, we'll still miss node B
      // Data should still be all available and the cluster is functional.
      createStatefulCacheManager("D", false);
      TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));

      assertOperationsFail();
      installTopologyOnCoordinator(false);

      waitForClusterToForm(CACHE_NAME);
      waitForStableTopology();

      checkData();
   }

   public void testCacheOperationsAfterSafeReinstall() {
      createInitialCluster();
      cache(0, CACHE_NAME).shutdown();
      TestingUtil.killCacheManagers(this.cacheManagers);
      assertStateFiles();
      this.cacheManagers.clear();

      // Restart 3 out of 4 nodes: 1 missing < 2 owners.
      // Topology should install safely without data loss.
      for (int i = 0; i < getClusterSize() - 1; i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }
      TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));

      assertOperationsFail();

      installTopologyOnCoordinator(false);
      waitForClusterToForm(CACHE_NAME);
      waitForStableTopology();

      checkData();

      // After rebalance, all segments must have numOwners copies.
      ConsistentHash ch = advancedCache(0, CACHE_NAME).getDistributionManager()
            .getCacheTopology().getWriteConsistentHash();
      for (int s = 0; s < ch.getNumSegments(); s++) {
         assertThat(ch.locateOwnersForSegment(s))
               .as("Segment %d should have %d owners after rebalance", s, NUM_OWNERS)
               .hasSize(NUM_OWNERS);
      }

      // Add the 4th node back and verify full cluster.
      createStatefulCacheManager("D", false);
      waitForClusterToForm(CACHE_NAME);
      checkData();
   }

   public void testCacheOperationsAfterForceReinstall() {
      createInitialCluster();
      cache(0, CACHE_NAME).shutdown();
      TestingUtil.killCacheManagers(this.cacheManagers);
      assertStateFiles();
      this.cacheManagers.clear();

      // Restart only 2 out of 4: 2 missing >= 2 owners.
      // The topology can only be installed with the force flag since it loses data.
      for (int i = 0; i < NUM_OWNERS; i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }
      TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));

      assertOperationsFail();

      Exceptions.expectException(MissingMembersException.class,
            "ISPN000694.*missing too many members.*",
            () -> installTopologyOnCoordinator(false));

      installTopologyOnCoordinator(true);
      waitForClusterToForm(CACHE_NAME);
      waitForStableTopology();

      // Force install loses data but cache must be operational.
      for (int i = 0; i < cacheManagers.size(); i++) {
         assertThat(cache(i, CACHE_NAME).size()).isGreaterThan(0);
      }
      assertCacheOperational();

      // Add remaining nodes.
      for (int i = NUM_OWNERS; i < getClusterSize(); i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }
      waitForClusterToForm(CACHE_NAME);

      for (int i = 0; i < getClusterSize(); i++) {
         assertThat(cache(i, CACHE_NAME).size()).isGreaterThan(0);
      }
   }

   private void installTopologyOnCoordinator(boolean force) {
      for (int j = 0; j < cacheManagers.size(); j++) {
         EmbeddedCacheManager ecm = manager(j);
         if (ecm.isCoordinator()) {
            boolean installed = TestingUtil.extractGlobalComponentRegistry(ecm)
                  .getClusterTopologyManager()
                  .useCurrentTopologyAsStable(CACHE_NAME, force);
            assertThat(installed).isTrue();
            return;
         }
      }
      throw new AssertionError("No coordinator found");
   }

   private void waitForStableTopology() {
      AggregateCompletionStage<Void> topologyInstall = CompletionStages.aggregateCompletionStage();
      for (int j = 0; j < cacheManagers.size(); j++) {
         GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(manager(j));
         topologyInstall.dependsOn(gcr.getLocalTopologyManager().stableTopologyCompletion(CACHE_NAME));
      }
      CompletableFutures.uncheckedAwait(topologyInstall.freeze().toCompletableFuture(), 30, TimeUnit.SECONDS);
   }

   private void assertOperationsFail() {
      for (int i = 0; i < cacheManagers.size(); i++) {
         for (int v = 0; v < DATA_SIZE; v++) {
            Cache<Object, Object> cache = cache(i, CACHE_NAME);
            String key = String.valueOf(v);
            Exceptions.expectException(MissingMembersException.class,
                  "ISPN000689: Recovering cache 'testCache' but there are missing members, known members \\[.*\\] of a total of 4$",
                  () -> cache.get(key));
         }
      }
   }

   private void assertStateFiles() {
      for (int i = 0; i < getClusterSize(); i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertThat(listFiles).as("State file should exist for node %d", i).hasSize(1);
      }
   }

   private void assertCacheOperational() {
      cache(0, CACHE_NAME).put("post-reinstall", "value");
      for (int i = 0; i < cacheManagers.size(); i++) {
         assertThat(cache(i, CACHE_NAME).get("post-reinstall")).isEqualTo("value");
      }
   }
}
