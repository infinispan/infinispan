package org.infinispan.partitionhandling;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashSet;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.LocalTopologyManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.DegradedJoinTest")
public class DegradedJoinTest extends BasePartitionHandlingTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new DegradedJoinTest().cacheMode(CacheMode.REPL_SYNC),
            new DegradedJoinTest().cacheMode(CacheMode.DIST_SYNC)
      };
   }

   public DegradedJoinTest() {
      numMembersInCluster = 2;
   }

   public void testSplitAndJoin() throws Exception {
      HashSet<Address> allMembers = new HashSet<>(asList(address(0), address(1)));
      //use set comparison as the merge view will reshuffle the order of nodes
      assertStableTopologyMembers(allMembers, partitionHandlingManager(0));
      assertStableTopologyMembers(allMembers, partitionHandlingManager(1));
      for (int i = 0; i < numMembersInCluster; i++) {
         assertEquals(AvailabilityMode.AVAILABLE, partitionHandlingManager(i).getAvailabilityMode());
      }

      // Split cluster
      PartitionDescriptor p0 = new PartitionDescriptor(0);
      PartitionDescriptor p1 = new PartitionDescriptor(1);
      splitCluster(p0.getNodes(), p1.getNodes());

      // Both nodes are in degraded mode
      partition(0).assertDegradedMode();
      partition(1).assertDegradedMode();
      assertStableTopologyMembers(allMembers, partitionHandlingManager(p1.node(0)));

      // Kill node 1
      manager(1).stop();
      enableDiscovery();

      // Start a new node
      ConfigurationBuilder dcc = cacheConfiguration();
      dcc.clustering().cacheMode(cacheMode).partitionHandling().whenSplit(partitionHandling).mergePolicy(mergePolicy);
      if (cacheMode == CacheMode.DIST_SYNC) {
         dcc.clustering().hash().numOwners(numberOfOwners);
      }
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().addContextInitializer(serializationContextInitializer());
      addClusterEnabledCacheManager(globalBuilder, dcc, new TransportFlags().withFD(true).withMerge(true));

      // Joiner can start, cache is still in degraded mode
      for (EmbeddedCacheManager manager : asList(manager(0), manager(2))) {
         AdvancedCache<Object, Object> cache = manager.getCache().getAdvancedCache();
         LocalizedCacheTopology cacheTopology = cache.getDistributionManager().getCacheTopology();
         assertEquals(singletonList(address(0)), cacheTopology.getActualMembers());
         assertEquals(asList(address(0), address(1)), cacheTopology.getMembers());

         PartitionHandlingManager partitionHandlingManager = extractComponent(cache, PartitionHandlingManager.class);
         assertEquals(AvailabilityMode.DEGRADED_MODE, partitionHandlingManager.getAvailabilityMode());
         assertStableTopologyMembers(allMembers, partitionHandlingManager);
      }

      // Make the cache available
      LocalTopologyManager localTopologyManager = extractGlobalComponent(manager(0), LocalTopologyManager.class);
      localTopologyManager.setCacheAvailability(getDefaultCacheName(), AvailabilityMode.AVAILABLE);

      // Now the joiner can receive state
      TestingUtil.waitForNoRebalance(cache(0), cache(2));
   }

   private void assertStableTopologyMembers(HashSet<Address> allMembers, PartitionHandlingManager phm) {
      assertEquals(allMembers, new HashSet<>(phm.getLastStableTopology().getMembers()));
   }

}
