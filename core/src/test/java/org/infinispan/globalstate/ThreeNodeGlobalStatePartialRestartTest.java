package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.util.Arrays;

import org.infinispan.Cache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.MissingMembersException;
import org.infinispan.topology.PersistentUUIDManager;
import org.testng.annotations.Test;

@Test(testName = "globalstate.ThreeNodeGlobalStatePartialRestartTest", groups = "functional")
public class ThreeNodeGlobalStatePartialRestartTest extends AbstractGlobalStateRestartTest {

   private PartitionHandling handling;
   private CacheMode cacheMode;
   private boolean purge;

   @Override
   protected int getClusterSize() {
      return 3;
   }

   @Override
   protected void applyCacheManagerClusteringConfiguration(ConfigurationBuilder config) {
      config.clustering().cacheMode(cacheMode).hash().numOwners(getClusterSize() - 1);
      config.clustering().partitionHandling().whenSplit(handling);
   }

   @Override
   protected void applyCacheManagerClusteringConfiguration(String id, ConfigurationBuilder config) {
      applyCacheManagerClusteringConfiguration(config);

      StoreConfigurationBuilder scb = config.persistence().addSoftIndexFileStore()
          .dataLocation(tmpDirectory(this.getClass().getSimpleName(), id, "data"))
          .indexLocation(tmpDirectory(this.getClass().getSimpleName(), id, "index"));
      scb.purgeOnStartup(purge);
   }

   public void testClusterDelayedJoiners() throws Exception {
      var addressMappings = createInitialCluster();

      ConsistentHash oldConsistentHash = advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();

      // Shutdown the cache cluster-wide
      cache(0, CACHE_NAME).shutdown();
      TestingUtil.killCacheManagers(this.cacheManagers);

      // Verify that the cache state file exists
      for (int i = 0; i < getClusterSize(); i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertEquals(Arrays.toString(listFiles), 1, listFiles.length);
      }
      this.cacheManagers.clear();

      // Partially recreate the cluster
      for (int i = 0; i < getClusterSize() - 1; i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }

      TestingUtil.blockUntilViewsReceived(30000, getCaches(CACHE_NAME));

      assertOperationsFail();

      // The last pending member joins.
      createStatefulCacheManager(Character.toString((char) ('A' + getClusterSize() - 1)), false);

      // Healthy cluster
      waitForClusterToForm(CACHE_NAME);

      checkClusterRestartedCorrectly(addressMappings);
      checkData();

      ConsistentHash newConsistentHash =
            advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      PersistentUUIDManager persistentUUIDManager = TestingUtil.extractGlobalComponent(manager(0), PersistentUUIDManager.class);
      assertEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager);
   }

   public void testConnectAndDisconnectDuringRestart() throws Exception {
      var addressMappings = createInitialCluster();

      ConsistentHash oldConsistentHash = advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();

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

      // Partially recreate the cluster.
      for (int i = 0; i < getClusterSize() - 1; i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }

      TestingUtil.blockUntilViewsReceived(30000, getCaches(CACHE_NAME));
      assertOperationsFail();

      // Stop one of the caches and assert the files are still here.
      EmbeddedCacheManager left = cacheManagers.remove(1);
      TestingUtil.killCacheManagers(left);
      String persistentLocation = left.getCacheManagerConfiguration().globalState().persistentLocation();
      File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
      assertEquals(Arrays.toString(listFiles), 1, listFiles.length);

      // Assert we are still unable to execute operations.
      assertOperationsFail();

      // Create the missing member instead of the one that left.
      createStatefulCacheManager(Character.toString((char) ('A' + getClusterSize() - 1)), false);
      TestingUtil.blockUntilViewsReceived(30000, getCaches(CACHE_NAME));

      // Assert we are still unable to execute operations.
      assertOperationsFail();

      // The missing restart again.
      createStatefulCacheManager(Character.toString((char) ('A' + 1)), false);

      // Healthy cluster
      waitForClusterToForm(CACHE_NAME);

      checkClusterRestartedCorrectly(addressMappings);
      checkData();

      ConsistentHash newConsistentHash =
          advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      PersistentUUIDManager persistentUUIDManager = TestingUtil.extractGlobalComponent(manager(0), PersistentUUIDManager.class);
      assertEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager);
   }

   public void testClusterWithRestartsDuringPartitioning() throws Exception {
      var addressMappings = createInitialCluster();

      ConsistentHash oldConsistentHash = advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();

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

      // Partially recreate the cluster.
      for (int i = 0; i < getClusterSize() - 1; i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }

      TestingUtil.blockUntilViewsReceived(30000, getCaches(CACHE_NAME));

      assertOperationsFail();

      // Shutdown the malformed cluster.
      cache(0, CACHE_NAME).shutdown();
      TestingUtil.killCacheManagers(this.cacheManagers);

      // Verify that the cache state file exists.
      // Since the topology was never restored, the state files should be present.
      for (int i = 0; i < getClusterSize() - 1; i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertEquals("Node " + i + " wrong files: " + Arrays.toString(listFiles), 1, listFiles.length);
      }
      this.cacheManagers.clear();

      // Recreate the complete cluster now.
      for (int i = 0; i < getClusterSize(); i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), false);
      }

      // Healthy cluster
      waitForClusterToForm(CACHE_NAME);

      checkClusterRestartedCorrectly(addressMappings);
      checkData();

      ConsistentHash newConsistentHash =
            advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      PersistentUUIDManager persistentUUIDManager = TestingUtil.extractGlobalComponent(manager(0), PersistentUUIDManager.class);
      assertEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager);
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

   public ThreeNodeGlobalStatePartialRestartTest withPartitionHandling(PartitionHandling handling) {
      this.handling = handling;
      return this;
   }

   public ThreeNodeGlobalStatePartialRestartTest withCacheMode(CacheMode mode) {
      this.cacheMode = mode;
      return this;
   }

   public ThreeNodeGlobalStatePartialRestartTest purgeOnStartup(boolean purge) {
      this.purge = purge;
      return this;
   }

   @Override
   public Object[] factory() {
      return Arrays.stream(PartitionHandling.values())
            .flatMap(ph -> Arrays.stream(new Object[] {
                  new ThreeNodeGlobalStatePartialRestartTest().withCacheMode(CacheMode.DIST_SYNC).withPartitionHandling(ph),
                  new ThreeNodeGlobalStatePartialRestartTest().withCacheMode(CacheMode.DIST_SYNC).withPartitionHandling(ph).purgeOnStartup(true),
                  new ThreeNodeGlobalStatePartialRestartTest().withCacheMode(CacheMode.REPL_SYNC).withPartitionHandling(ph).purgeOnStartup(true),
                  new ThreeNodeGlobalStatePartialRestartTest().withCacheMode(CacheMode.REPL_SYNC).withPartitionHandling(ph),
            }))
            .toArray();
   }

   @Override
   protected String parameters() {
      return String.format("[cacheMode=%s, ph=%s, purge=%b]", cacheMode, handling, purge);
   }
}
