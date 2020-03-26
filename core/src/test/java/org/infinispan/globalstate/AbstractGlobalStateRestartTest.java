package org.infinispan.globalstate;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;

public abstract class AbstractGlobalStateRestartTest extends MultipleCacheManagersTest {

   public static int DATA_SIZE = 100;

   private static final String CACHE_NAME = "testCache";

   protected abstract int getClusterSize();

   @Override
   protected boolean cleanupAfterMethod() {
      return true;
   }

   @Override
   protected boolean cleanupAfterTest() {
      return false;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManagers(true, -1, false);
   }

   protected void createStatefulCacheManagers(boolean clear, int extraneousNodePosition, boolean reverse) {
      int totalNodes = getClusterSize() + ((extraneousNodePosition < 0) ? 0 : 1);
      int node = reverse ? getClusterSize() - 1 : 0;
      int step = reverse ? -1 : 1;
      for (int i = 0; i < totalNodes; i++) {
         if (i == extraneousNodePosition) {
            // Create one more node if needed in the requested position
            createStatefulCacheManager(Character.toString('@'), true);
         } else {
            createStatefulCacheManager(Character.toString((char) ('A' + node)), clear);
            node += step;
         }
      }
   }

   private void createStatefulCacheManager(String id, boolean clear) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      ConfigurationBuilder config = new ConfigurationBuilder();
      applyCacheManagerClusteringConfiguration(config);
      config.persistence().addSingleFileStore().location(stateDirectory);
      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);
      manager.defineConfiguration(CACHE_NAME, config.build());
   }

   protected abstract void applyCacheManagerClusteringConfiguration(ConfigurationBuilder config);

   protected void shutdownAndRestart(int extraneousNodePosition, boolean reverse) throws Throwable {
      Map<JGroupsAddress, PersistentUUID> addressMappings = createInitialCluster();

      ConsistentHash oldConsistentHash = advancedCache(0, CACHE_NAME).getDistributionManager().getWriteConsistentHash();

      // Shutdown the cache cluster-wide
      cache(0, CACHE_NAME).shutdown();

      TestingUtil.killCacheManagers(this.cacheManagers);

      // We should have some data here
      for (int i = 0; i < getClusterSize(); i++) {
         checkStateDirNotEmpty(manager(i).getCacheManagerConfiguration().globalState().persistentLocation());
      }
      this.cacheManagers.clear();

      // Recreate the cluster
      createStatefulCacheManagers(false, extraneousNodePosition, reverse);
      if(reverse) {
         Map<JGroupsAddress, PersistentUUID> reversed = new LinkedHashMap<>();
         reverseLinkedMap(addressMappings.entrySet().iterator(), reversed);
         addressMappings = reversed;
      }

      // Healthy cluster
      switch (extraneousNodePosition) {
         case -1: {
            // Healthy cluster
            waitForClusterToForm(CACHE_NAME);

            checkClusterRestartedCorrectly(addressMappings);
            checkData();

            ConsistentHash newConsistentHash =
                  advancedCache(0, CACHE_NAME).getDistributionManager().getWriteConsistentHash();
            PersistentUUIDManager persistentUUIDManager = TestingUtil.extractGlobalComponent(manager(0), PersistentUUIDManager.class);
            assertEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager);
            break;
         }
         case 0: {
            // Coordinator without state, all other nodes will break
            for(int i = 1; i < cacheManagers.size(); i++) {
               try {
                  cache(i, CACHE_NAME);
                  fail("Cache with state should not have joined coordinator without state");
               } catch (CacheException e) {
                  // Ignore
                  log.debugf("Got expected exception: %s", e);
               }
            }
            break;
         }
         default: {
            // Other node without state
            try {
               cache(extraneousNodePosition, CACHE_NAME);
               fail("Cache without state should not have joined coordinator with state");
            } catch (CacheException e) {
               // Ignore
            }
         }
      }
   }

   private void assertEquivalent(Map<JGroupsAddress, PersistentUUID> addressMappings,
         ConsistentHash oldConsistentHash, ConsistentHash newConsistentHash,
         PersistentUUIDManager persistentUUIDManager) {
      assertTrue(isEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager));
   }

   private void checkClusterRestartedCorrectly(Map<JGroupsAddress, PersistentUUID> addressMappings) throws Exception {
      Iterator<Map.Entry<JGroupsAddress, PersistentUUID>> addressIterator = addressMappings.entrySet().iterator();
      for (int i = 0; i < cacheManagers.size(); i++) {
         LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(i), LocalTopologyManager.class);
         // Ensure that nodes have the old UUID
         assertEquals(addressIterator.next().getValue(), ltm.getPersistentUUID());
         // Ensure that rebalancing is enabled for the cache
         assertTrue(ltm.isCacheRebalancingEnabled(CACHE_NAME));
      }
   }

   private void checkData() {
      // Ensure that the cache contains the right data
      assertEquals(DATA_SIZE, cache(0, CACHE_NAME).size());
      for (int i = 0; i < DATA_SIZE; i++) {
         assertEquals(cache(0, CACHE_NAME).get(String.valueOf(i)), String.valueOf(i));
      }
   }

   private Map<JGroupsAddress, PersistentUUID> createInitialCluster() {
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

   private void fillData() {
      // Fill some data
      for (int i = 0; i < DATA_SIZE; i++) {
         cache(0, CACHE_NAME).put(String.valueOf(i), String.valueOf(i));
      }
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

   private void checkStateDirNotEmpty(String location) {
      File[] listFiles = new File(location).listFiles();
      assertTrue(listFiles.length > 0);
   }

   private void reverseLinkedMap(Iterator<Map.Entry<JGroupsAddress, PersistentUUID>> iterator, Map<JGroupsAddress, PersistentUUID> reversed) {
      if (iterator.hasNext()) {
         Map.Entry<JGroupsAddress, PersistentUUID> entry = iterator.next();
         reverseLinkedMap(iterator, reversed);
         reversed.put(entry.getKey(), entry.getValue());
      }
   }
}
