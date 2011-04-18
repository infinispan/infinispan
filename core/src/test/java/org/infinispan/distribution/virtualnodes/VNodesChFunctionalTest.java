package org.infinispan.distribution.virtualnodes;

import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.DistSyncFuncTest;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.distribution.ch.TopologyInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "distribution.VNodesChFunctionalTest")
public class VNodesChFunctionalTest extends DistSyncFuncTest {
   
   public VNodesChFunctionalTest() {
      numVirtualNodes = 10;
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
      int index = cacheManagers.size();
      String rack;
      String machine;
      switch (index) {
         case 0 : {
            rack = "r0";
            machine = "m0";
            break;
         }
         case 1 : {
            rack = "r0";
            machine = "m1";
            break;
         }
         case 2 : {
            rack = "r1";
            machine = "m0";
            break;
         }
         case 3 : {
            rack = "r2";
            machine = "m0";
            break;
         }
         default : {
            throw new RuntimeException("Bad!");
         }
      }
      GlobalConfiguration globalConfiguration = cm.getGlobalConfiguration();      
      globalConfiguration.setRackId(rack);
      globalConfiguration.setMachineId(machine);
      cacheManagers.add(cm);
      return cm;
   }

   public void testHashesInitiated() {
      TopologyAwareConsistentHash hash = (TopologyAwareConsistentHash) advancedCache(0, cacheName).getDistributionManager().getConsistentHash();
      containsAllHashes(hash);
      containsAllHashes((TopologyAwareConsistentHash) advancedCache(1, cacheName).getDistributionManager().getConsistentHash());
      containsAllHashes((TopologyAwareConsistentHash) advancedCache(2, cacheName).getDistributionManager().getConsistentHash());
      containsAllHashes((TopologyAwareConsistentHash) advancedCache(3, cacheName).getDistributionManager().getConsistentHash());
   }

   private void containsAllHashes(TopologyAwareConsistentHash ch) {
      assert ch.getCaches().contains(address(0));
      assert ch.getCaches().contains(address(1));
      assert ch.getCaches().contains(address(2));
      assert ch.getCaches().contains(address(3));
      TopologyInfo topologyInfo = ch.getTopologyInfo();
      assert topologyInfo.containsInfoForNode(address(0)) : topologyInfo;
      assert topologyInfo.containsInfoForNode(address(1)) : topologyInfo;
      assert topologyInfo.containsInfoForNode(address(2)) : topologyInfo;
      assert topologyInfo.containsInfoForNode(address(3)) : topologyInfo;
   }
}
