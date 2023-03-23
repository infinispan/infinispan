package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.MissingMembersException;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
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

   @Override
   protected void applyCacheManagerClusteringConfiguration(String id, ConfigurationBuilder config) {
      applyCacheManagerClusteringConfiguration(config);
      config.persistence().addSoftIndexFileStore()
            .dataLocation(tmpDirectory(this.getClass().getSimpleName(), id, "data"))
            .indexLocation(tmpDirectory(this.getClass().getSimpleName(), id, "index"));
   }

   public void testReinstallTopologyByForce() throws Exception {
      executeTestRestart(true);
   }

   public void testReinstallTopologySafely() throws Exception {
      executeTestRestart(false);
   }

   private void executeTestRestart(boolean force) throws Exception {
      boolean possibleDataLoss = !cacheMode.isReplicated() && force;
      Map<JGroupsAddress, PersistentUUID> addressMappings = createInitialCluster();

      // Shutdown the cache cluster-wide
      cache(0, CACHE_NAME).shutdown();
      TestingUtil.killCacheManagers(this.cacheManagers);

      // Verify that the cache state file exists for all participants.
      for (int i = 0; i < getClusterSize(); i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertEquals(Arrays.toString(listFiles), 1, listFiles.length);
      }
      this.cacheManagers.clear();

      // Partially recreate the cluster while trying to reinstall topology.
      int i = 0;
      for (; i < (getClusterSize() - getNumOwners()); i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
         TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));
         GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(manager(i));
         Exceptions.expectException(MissingMembersException.class,
               "ISPN000694: Cache 'testCache' has number of owners \\d but is missing too many members \\(\\d\\/3\\) to reinstall topology$",
               () -> gcr.getClusterTopologyManager().useCurrentTopologyAsStable(CACHE_NAME, false));
      }

      // Since we didn't force the installation, operations still fail.
      assertOperationsFail();

      if (!force) {
         createStatefulCacheManager(Character.toString((char) ('A' + i++)), false);
         TestingUtil.blockUntilViewsReceived(15000, getCaches(CACHE_NAME));
      }

      // Now we install the topology.
      for (int j = 0; j < cacheManagers.size(); j++) {
         EmbeddedCacheManager ecm = manager(j);
         if (ecm.isCoordinator()) {
            TestingUtil.extractGlobalComponentRegistry(manager(0))
                  .getClusterTopologyManager()
                  .useCurrentTopologyAsStable(CACHE_NAME, force);
            break;
         }
      }

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

      // Now we add the missing members back.
      for (; i < getClusterSize(); i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }

      // This will create the cache, and trigger the join operation for the new managers.
      TestingUtil.blockUntilViewsReceived(30000, getCaches(CACHE_NAME));

      // Wait topology again, just to make sure.
      topologyInstall = CompletionStages.aggregateCompletionStage();
      for (int j = 0; j < cacheManagers.size(); j++) {
         GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(manager(j));
         topologyInstall.dependsOn(gcr.getLocalTopologyManager().stableTopologyCompletion(CACHE_NAME));
      }
      CompletableFutures.uncheckedAwait(topologyInstall.freeze().toCompletableFuture(), 30, TimeUnit.SECONDS);

      checkClusterRestartedCorrectly(addressMappings);
      waitForClusterToForm(CACHE_NAME);

      if (possibleDataLoss) {
         for (int j = 0; j < getClusterSize(); j++) {
            assertFalse(cache(j, CACHE_NAME).isEmpty());
            assertTrue(cache(j, CACHE_NAME).size() < DATA_SIZE);
         }
      } else {
         checkData();
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
