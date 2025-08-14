package org.infinispan.partitionhandling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.UncleanShutdownAction;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.LocalTopologyManager;
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
      global.globalState().enable().persistentLocation(stateDirectory).uncleanShutdownAction(UncleanShutdownAction.IGNORE);
      global.serialization().addContextInitializer(TestDataSCI.INSTANCE);

      ConfigurationBuilder config = new ConfigurationBuilder();
      partitionHandlingBuilder(config);
      config.persistence().addSoftIndexFileStore();

      ConfigurationBuilder defaultConfig = createDefault ? config : null;
      EmbeddedCacheManager manager = createClusteredCacheManager(start, global, defaultConfig, new TransportFlags());
      cacheManagers.add(manager);
      if (start) manager.defineConfiguration(CACHE_NAME, config.build());
   }

   protected Map<Address, UUID> createInitialCluster() {
      waitForClusterToForm(CACHE_NAME);
      Map<Address, UUID> addressMappings = new LinkedHashMap<>();

      for (int i = 0; i < numMembersInCluster; i++) {
         LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(i), LocalTopologyManager.class);
         var uuid = ltm.getPersistentUUID();
         assertNotNull(uuid);
         addressMappings.put(manager(i).getAddress(), uuid);
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

   protected void assertHealthyCluster(Map<Address, UUID> addressMappings, ConsistentHash oldConsistentHash) throws Throwable {
      // Healthy cluster
      waitForClusterToForm(CACHE_NAME);

      checkClusterRestartedCorrectly(addressMappings);
      checkData();

      ConsistentHash newConsistentHash =
            advancedCache(0, CACHE_NAME).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      PersistentUUIDManager persistentUUIDManager = TestingUtil.extractGlobalComponent(manager(0), PersistentUUIDManager.class);
      assertEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager);
   }

   void checkClusterRestartedCorrectly(Map<Address, UUID> addressMappings) throws Exception {
      checkPersistentUUIDMatch(addressMappings);
      checkClusterDataSize(DATA_SIZE);
   }

   void checkClusterDataSize(int expectedSize) {
      SoftAssertions sa = new SoftAssertions();
      int size = expectedSize;
      for (int i = 0; i < cacheManagers.size(); i++) {
         int s = cache(i, CACHE_NAME).size();
         if (size < 0) size = s;

         sa.assertThat(s)
               .withFailMessage(String.format("Manager %s has size %d instead of %d for '%s'", manager(i).getAddress(), s, size, CACHE_NAME))
               .isEqualTo(size);
      }

      sa.assertAll();
   }

   void checkPersistentUUIDMatch(Map<Address, UUID> addressMappings) throws Exception {
      var addressIterator = addressMappings.entrySet().iterator();
      Set<UUID> uuids = new HashSet<>();
      for (int i = 0; i < cacheManagers.size(); i++) {
         LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(i), LocalTopologyManager.class);
         assertTrue(uuids.add(ltm.getPersistentUUID()));
      }

      for (int i = 0; i < cacheManagers.size(); i++) {
         LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(i), LocalTopologyManager.class);
         // Ensure that nodes have the old UUID
         var entry = addressIterator.next();
         assertTrue(entry.getKey() + " is mapping to the wrong UUID: " +
               "Expected: " + entry.getValue() + " not found in: " + uuids, uuids.contains(entry.getValue()));
         // Ensure that rebalancing is enabled for the cache
         assertTrue(ltm.isCacheRebalancingEnabled(CACHE_NAME));
      }
   }

   void assertEquivalent(Map<Address, UUID> addressMappings, ConsistentHash oldConsistentHash,
                         ConsistentHash newConsistentHash, PersistentUUIDManager persistentUUIDManager) {
      assertTrue(isEquivalent(addressMappings, oldConsistentHash, newConsistentHash, persistentUUIDManager));
   }

   protected final boolean isEquivalent(Map<Address, UUID> addressMapping, ConsistentHash oldConsistentHash,
                                        ConsistentHash newConsistentHash, PersistentUUIDManager persistentUUIDManager) {
      if (oldConsistentHash.getNumSegments() != newConsistentHash.getNumSegments()) return false;
      for (int i = 0; i < oldConsistentHash.getMembers().size(); i++) {
         Address oldAddress = oldConsistentHash.getMembers().get(i);
         Address remappedOldAddress = persistentUUIDManager.getAddress(addressMapping.get(oldAddress));
         Address newAddress = newConsistentHash.getMembers().get(i);
         if (!remappedOldAddress.equals(newAddress)) return false;
         Set<Integer> oldSegmentsForOwner = oldConsistentHash.getSegmentsForOwner(oldAddress);
         Set<Integer> newSegmentsForOwner = newConsistentHash.getSegmentsForOwner(newAddress);
         assertThat(oldSegmentsForOwner)
               .withFailMessage(() -> String.format("Old: %s\nNew: %s", IntSets.from(oldSegmentsForOwner), IntSets.from(newSegmentsForOwner)))
               .isEqualTo(newSegmentsForOwner);
      }

      return true;
   }
}
