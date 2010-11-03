package org.infinispan.distribution.topologyaware;

import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.DistAsyncFuncTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "topologyaware.TopologyAwareDistAsyncFuncTest")
public class TopologyAwareDistAsyncFuncTest extends DistAsyncFuncTest {

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
            rack = "r1";
            machine = "m0";
            break;
         }
         case 2 : {
            rack = "r1";
            machine = "m0";
            break;
         }
         case 3 : {
            rack = "r1";
            machine = "m1";
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
}
