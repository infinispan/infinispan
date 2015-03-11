package org.infinispan.distribution.topologyaware;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistSyncFuncTest;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "distribution.topologyaware.TopologyAwareChFunctionalTest")
public class TopologyAwareChFunctionalTest extends DistSyncFuncTest {

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags) {
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
      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gc.transport().rackId(rack).machineId(machine);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(gc, new ConfigurationBuilder());
      cacheManagers.add(cm);
      return cm;
   }

   public void testHashesInitiated() {
      ConsistentHash hash = advancedCache(0, cacheName).getDistributionManager().getWriteConsistentHash();
      containsAllHashes(hash);
      containsAllHashes(advancedCache(1, cacheName).getDistributionManager().getWriteConsistentHash());
      containsAllHashes(advancedCache(2, cacheName).getDistributionManager().getWriteConsistentHash());
      containsAllHashes(advancedCache(3, cacheName).getDistributionManager().getWriteConsistentHash());
   }

   private void containsAllHashes(ConsistentHash ch) {
      assert ch.getMembers().contains(address(0));
      assert ch.getMembers().contains(address(1));
      assert ch.getMembers().contains(address(2));
      assert ch.getMembers().contains(address(3));
   }
}
