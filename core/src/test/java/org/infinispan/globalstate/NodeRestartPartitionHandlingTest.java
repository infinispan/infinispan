package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.MissingMembersException;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "globalstate.NodeRestartPartitionHandlingTest")
public class NodeRestartPartitionHandlingTest extends BasePartitionHandlingTest {

   public static final int DATA_SIZE = 100;
   public static final String CACHE_NAME = "testCache";

   {
      partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
      numMembersInCluster = 2;
   }

   protected int getClusterSize() {
      return numMembersInCluster;
   }

   @Override
   protected String customCacheName() {
      return CACHE_NAME;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManagers(true);
   }

   public void testRestartDuringNetworkPartition() throws Throwable {
      Map<JGroupsAddress, PersistentUUID> addressMappings = createInitialCluster();
      ConsistentHash oldConsistentHash = advancedCache(0, CACHE_NAME).getDistributionManager().getWriteConsistentHash();

      for (int i = 0; i < getClusterSize(); i++) {
         ((DefaultCacheManager) manager(i)).shutdownAllCaches();
      }

      TestingUtil.killCacheManagers(this.cacheManagers);

      // Verify that the cache state file exists
      for (int i = 0; i < getClusterSize(); i++) {
         String persistentLocation = manager(i).getCacheManagerConfiguration().globalState().persistentLocation();
         File[] listFiles = new File(persistentLocation).listFiles((dir, name) -> name.equals(CACHE_NAME + ".state"));
         assertEquals(Arrays.toString(listFiles), 1, listFiles.length);
      }
      cacheManagers.clear();

      createStatefulCacheManagers(false);

      // We split the cluster. This should make the caches not be able to restore.
      splitCluster(new int[]{0}, new int[]{1});
      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();

      // We restart the cluster, completely. Caches should issue join requests during partition.
      for (int i = 0; i < getClusterSize(); i++) {
         cache(i, CACHE_NAME);
      }

      // Assert we still partitioned.
      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();

      // Since the cluster is partitioned, the cache didn't recovered, operations should fail.
      assertOperationsFail();

      // Merge the cluster. This should make the caches restore.
      partition(0).merge(partition(1), false);
      waitForClusterToForm(CACHE_NAME);
      assertHealthyCluster(addressMappings, oldConsistentHash);
   }

   protected void createStatefulCacheManagers(boolean clear) {
      for (int i = 0; i < getClusterSize(); i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), clear);
      }
   }

   void createStatefulCacheManager(String id, boolean clear) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      ConfigurationBuilder config = new ConfigurationBuilder();
      partitionHandlingBuilder(config);
      config.persistence().addSingleFileStore().location(stateDirectory).fetchPersistentState(true);
      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);
      manager.defineConfiguration(CACHE_NAME, config.build());
   }

   Map<JGroupsAddress, PersistentUUID> createInitialCluster() {
      waitForClusterToForm(CACHE_NAME);
      Map<JGroupsAddress, PersistentUUID> addressMappings = new LinkedHashMap<>();

      for (int i = 0; i < getClusterSize(); i++) {
         LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(i), LocalTopologyManager.class);
         PersistentUUID uuid = ltm.getPersistentUUID();
         assertNotNull(uuid);
         addressMappings.put((JGroupsAddress) manager(i).getAddress(), uuid);
      }

      fillData();
      checkData();

      return addressMappings;
   }

   private void assertOperationsFail() {
      for (int i = 0; i < cacheManagers.size(); i++) {
         for (int v = 0; v < DATA_SIZE; v++) {
            final Cache<Object, Object> cache = cache(i, CACHE_NAME);
            String key = String.valueOf(v);
            // Always returns null. Message about not stable yet is logged.
            Exceptions.expectException(MissingMembersException.class,
                  "ISPN000689: Recovering cache 'testCache' but there are missing members, known members \\[.*\\] of a total of 2$",
                  () -> cache.get(key));
         }
      }
   }

   private void fillData() {
      // Fill some data
      for (int i = 0; i < DATA_SIZE; i++) {
         cache(0, CACHE_NAME).put(String.valueOf(i), String.valueOf(i));
      }
   }

   void checkData() {
      // Ensure that the cache contains the right data
      assertEquals(DATA_SIZE, cache(0, CACHE_NAME).size());
      for (int i = 0; i < DATA_SIZE; i++) {
         assertEquals(cache(0, CACHE_NAME).get(String.valueOf(i)), String.valueOf(i));
      }
   }

   protected void assertHealthyCluster(Map<JGroupsAddress, PersistentUUID> addressMappings, ConsistentHash oldConsistentHash) throws Throwable {
      // Healthy cluster
      waitForClusterToForm(CACHE_NAME);

      checkClusterRestartedCorrectly(addressMappings);
      checkData();

      ConsistentHash newConsistentHash =
            advancedCache(0, CACHE_NAME).getDistributionManager().getWriteConsistentHash();
      PersistentUUIDManager persistentUUIDManager = TestingUtil.extractGlobalComponent(manager(0), PersistentUUIDManager.class);
      assertEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager);
   }

   void checkClusterRestartedCorrectly(Map<JGroupsAddress, PersistentUUID> addressMappings) throws Exception {
      Iterator<Map.Entry<JGroupsAddress, PersistentUUID>> addressIterator = addressMappings.entrySet().iterator();
      Set<PersistentUUID> uuids = new HashSet<>();
      for (int i = 0; i < cacheManagers.size(); i++) {
         LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(i), LocalTopologyManager.class);
         assertTrue(uuids.add(ltm.getPersistentUUID()));
      }

      for (int i = 0; i < cacheManagers.size(); i++) {
         LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(i), LocalTopologyManager.class);
         // Ensure that nodes have the old UUID
         Map.Entry<JGroupsAddress, PersistentUUID> entry = addressIterator.next();
         assertTrue(entry.getKey() + " is mapping to the wrong UUID: " +
               "Expected: " + entry.getValue() + " not found in: " + uuids, uuids.contains(entry.getValue()));
         // Ensure that rebalancing is enabled for the cache
         assertTrue(ltm.isCacheRebalancingEnabled(CACHE_NAME));
      }
   }

   void assertEquivalent(Map<JGroupsAddress, PersistentUUID> addressMappings, ConsistentHash oldConsistentHash,
                         ConsistentHash newConsistentHash, PersistentUUIDManager persistentUUIDManager) {
      assertTrue(isEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager));
   }

   private boolean isEquivalent(Map<JGroupsAddress, PersistentUUID> addressMapping, ConsistentHash oldConsistentHash, ConsistentHash newConsistentHash, PersistentUUIDManager persistentUUIDManager) {
      if (oldConsistentHash.getNumSegments() != newConsistentHash.getNumSegments()) return false;
      for (int i = 0; i < oldConsistentHash.getMembers().size(); i++) {
         JGroupsAddress oldAddress = (JGroupsAddress) oldConsistentHash.getMembers().get(i);
         JGroupsAddress remappedOldAddress = (JGroupsAddress) persistentUUIDManager.getAddress(addressMapping.get(oldAddress));
         JGroupsAddress newAddress = (JGroupsAddress) newConsistentHash.getMembers().get(i);
         if (!remappedOldAddress.equals(newAddress)) return false;
         Set<Integer> oldSegmentsForOwner = oldConsistentHash.getSegmentsForOwner(oldAddress);
         Set<Integer> newSegmentsForOwner = newConsistentHash.getSegmentsForOwner(newAddress);
         if (!oldSegmentsForOwner.equals(newSegmentsForOwner)) return false;
      }

      return true;
   }
}
