package org.infinispan.partitionhandling;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;

public class BaseStatefulPartitionHandlingTest extends BasePartitionHandlingTest {

   protected static final int DATA_SIZE = 100;
   protected static final String CACHE_NAME = "testCache";

   protected boolean createDefault;

   @Override
   protected String customCacheName() {
      return CACHE_NAME;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCacheManagers(true);
   }

   protected void createStatefulCacheManagers(boolean clear) {
      for (int i = 0; i < numMembersInCluster; i++) {
         createStatefulCacheManager(Character.toString((char) ('A' + i)), clear);
      }
   }

   void createStatefulCacheManager(String id, boolean clear) {
      createStatefulCacheManager(id, clear, true);
   }

   final void createStatefulCacheManager(String id, boolean clear, boolean start) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);
      global.serialization().addContextInitializer(TestDataSCI.INSTANCE);

      ConfigurationBuilder config = new ConfigurationBuilder();
      partitionHandlingBuilder(config);
      config.persistence().addSingleFileStore().location(stateDirectory).fetchPersistentState(true);

      ConfigurationBuilder defaultConfig = createDefault ? config : null;
      EmbeddedCacheManager manager = createClusteredCacheManager(start, global, defaultConfig, new TransportFlags());
      cacheManagers.add(manager);
      if (start) manager.defineConfiguration(CACHE_NAME, config.build());
   }

   protected Map<JGroupsAddress, PersistentUUID> createInitialCluster() {
      waitForClusterToForm(CACHE_NAME);
      Map<JGroupsAddress, PersistentUUID> addressMappings = new LinkedHashMap<>();

      for (int i = 0; i < numMembersInCluster; i++) {
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

   private boolean isEquivalent(Map<JGroupsAddress, PersistentUUID> addressMapping, ConsistentHash oldConsistentHash,
                                ConsistentHash newConsistentHash, PersistentUUIDManager persistentUUIDManager) {
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
