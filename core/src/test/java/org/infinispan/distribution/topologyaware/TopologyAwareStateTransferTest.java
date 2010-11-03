package org.infinispan.distribution.topologyaware;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "topologyaware.TopologyAwareStateTransferTest")
public class TopologyAwareStateTransferTest extends MultipleCacheManagersTest {

   private Address a0;
   private Address a1;
   private Address a2;
   private Address a3;
   private Address a4;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration defaultConfig = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      System.out.println("defaultConfig = " + defaultConfig.getNumOwners());
      defaultConfig.setL1CacheEnabled(false);
      createClusteredCaches(5, defaultConfig);
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(0), cache(1), cache(2), cache(3), cache(4));

      TopologyAwareConsistentHash hash =
            (TopologyAwareConsistentHash) cache(0).getAdvancedCache().getDistributionManager().getConsistentHash();
      List<Address> addressList = hash.getCaches();
      System.out.println("addressList = " + addressList);
      a0 = addressList.get(0);
      a1 = addressList.get(1);
      a2 = addressList.get(2);
      a3 = addressList.get(3);
      a4 = addressList.get(4);
      printTopologyInfo("a0", cache(a0).getConfiguration().getGlobalConfiguration());
      printTopologyInfo("a1", cache(a1).getConfiguration().getGlobalConfiguration());
      printTopologyInfo("a2", cache(a2).getConfiguration().getGlobalConfiguration());
      printTopologyInfo("a3", cache(a3).getConfiguration().getGlobalConfiguration());
      printTopologyInfo("a4", cache(a4).getConfiguration().getGlobalConfiguration());
   }

   private void printTopologyInfo(String str, GlobalConfiguration gc) {
      System.out.println(str + ": " + gc.getRackId() + "->" + gc.getMachineId());
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {      
   }

   Cache cache(Address addr) {
      for (Cache c : caches()) {
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(addr)) return c;
      }
      throw new RuntimeException("Address: " + addr);
   }

   public void testInitialState() {
      cache(0).put(a0,"v0");
      cache(0).put(a1,"v0");
      cache(0).put(a2,"v0");
      cache(0).put(a3,"v0");
      cache(0).put(a4,"v0");
      assertExistence(a0);
      assertExistence(a1);
      assertExistence(a2);
      assertExistence(a3);
      assertExistence(a4);
   }

   @Test (dependsOnMethods = "testInitialState")
   public void testNodeDown() {
      EmbeddedCacheManager cm = (EmbeddedCacheManager) cache(a4).getCacheManager();
      log.info("Here is where ST starts");
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(a0), cache(a1), cache(a2), cache(a3));
      log.info("Here is where ST ends");
      List<Address> addressList = cache(a0).getAdvancedCache().getDistributionManager().getConsistentHash().getCaches();
      System.out.println("After shutting down " + a4 + " caches are " +  addressList);


      System.out.println(TestingUtil.printCache(cache(a0)));
      System.out.println(TestingUtil.printCache(cache(a1)));
      System.out.println(TestingUtil.printCache(cache(a2)));
      System.out.println(TestingUtil.printCache(cache(a3)));

      assertExistence(a0);
      assertExistence(a1);
      assertExistence(a2);
      assertExistence(a3);
      assertExistence(a4);      
   }

   @Test (dependsOnMethods = "testNodeDown")
   public void testNodeDown2() {
      EmbeddedCacheManager cm = (EmbeddedCacheManager) cache(a2).getCacheManager();
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(a0), cache(a1), cache(a3));
      assertExistence(a0);
      assertExistence(a1);
      assertExistence(a2);
      assertExistence(a3);
      assertExistence(a4);
   }

   @Test (dependsOnMethods = "testNodeDown2")
   public void testNodeDown3() {
      EmbeddedCacheManager cm = (EmbeddedCacheManager) cache(a1).getCacheManager();
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(a0), cache(a3));
      assertExistence(a0);
      assertExistence(a1);
      assertExistence(a2);
      assertExistence(a3);
      assertExistence(a4);
   }


   private void assertExistence(final Object key) {
      ConsistentHash hash = cache(a0).getAdvancedCache().getDistributionManager().getConsistentHash();
      final List<Address> addresses = hash.locate(key, 2);
      System.out.println(key + " should be present on = " + addresses);
      log.info(key + " should be present on = " + addresses);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int count = 0;
            for (Cache c : caches()) {
               if (c.getAdvancedCache().getDataContainer().containsKey(key)) {
                  System.out.println("It is here = " + address(c));
                  count++;
               }
            }
            System.out.println("count = " + count);
            return count == 2;
         }         
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (Cache c : caches()) {
               if (addresses.contains(address(c))) {
                  if (!c.getAdvancedCache().getDataContainer().containsKey(key)) {
                     System.out.println(key + " not present on " + c.getAdvancedCache().getRpcManager().getAddress());
                     return false;
                  }
               } else {
                  if (c.getAdvancedCache().getDataContainer().containsKey(key)) {
                     System.out.println(key + " present on " + c.getAdvancedCache().getRpcManager().getAddress());
                     return false;
                  }
               }
            }
            return true;
         }
      });
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration deConfiguration) {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(deConfiguration);
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
         case 4 : {
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
}
