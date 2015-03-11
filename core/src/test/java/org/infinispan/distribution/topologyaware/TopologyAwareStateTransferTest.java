package org.infinispan.distribution.topologyaware;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.topologyaware.TopologyAwareStateTransferTest")
@CleanupAfterTest
public class TopologyAwareStateTransferTest extends MultipleCacheManagersTest {

   private Address[] addresses;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      log.debug("defaultConfig = " + defaultConfig.build().clustering().hash().numOwners());
      defaultConfig.clustering().l1().disable().stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(5, defaultConfig);

      ConsistentHash hash = cache(0).getAdvancedCache().getDistributionManager().getWriteConsistentHash();
      List<Address> members = hash.getMembers();
      addresses = members.toArray(new Address[members.size()]);
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   Cache<?, ?> cache(Address addr) {
      for (Cache<?, ?> c : caches()) {
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(addr)) return c;
      }
      throw new RuntimeException("Address: " + addr);
   }

   public void testInitialState() {
      cache(0).put(addresses[0],"v0");
      cache(0).put(addresses[1],"v1");
      cache(0).put(addresses[2],"v2");
      cache(0).put(addresses[3],"v3");
      cache(0).put(addresses[4],"v4");

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[1], TestingUtil.printCache(cache(addresses[1])));
      log.debugf("Cache on node %s: %s", addresses[2], TestingUtil.printCache(cache(addresses[2])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);
   }

   @Test (dependsOnMethods = "testInitialState")
   public void testNodeDown() {
      EmbeddedCacheManager cm = cache(addresses[4]).getCacheManager();
      log.info("Here is where ST starts");
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForRehashToComplete(caches());
      log.info("Here is where ST ends");
      List<Address> addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager().getWriteConsistentHash().getMembers();
      log.debug("After shutting down " + addresses[4] + " caches are " +  addressList);

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[1], TestingUtil.printCache(cache(addresses[1])));
      log.debugf("Cache on node %s: %s", addresses[2], TestingUtil.printCache(cache(addresses[2])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);
   }

   @Test (dependsOnMethods = "testNodeDown")
   public void testNodeDown2() {
      EmbeddedCacheManager cm = cache(addresses[2]).getCacheManager();
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForRehashToComplete(caches());
      List<Address> addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager().getWriteConsistentHash().getMembers();
      log.debug("After shutting down " + addresses[2] + " caches are " +  addressList);

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[1], TestingUtil.printCache(cache(addresses[1])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);
   }

   @Test (dependsOnMethods = "testNodeDown2")
   public void testNodeDown3() {
      EmbeddedCacheManager cm = cache(addresses[1]).getCacheManager();
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForRehashToComplete(caches());
      List<Address> addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager().getWriteConsistentHash().getMembers();
      log.debug("After shutting down " + addresses[1] + " caches are " +  addressList);

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);
   }


   private <K> void assertExistence(final K key) {
      org.infinispan.distribution.ch.ConsistentHash hash = cache(addresses[0]).getAdvancedCache().getDistributionManager().getWriteConsistentHash();
      final List<Address> addresses = hash.locateOwners(key);
      log.debug(key + " should be present on = " + addresses);

      int count = 0;
      for (Cache<? super K, ?> c : caches()) {
         if (c.getAdvancedCache().getDataContainer().containsKey(key)) {
            log.debug("It is here = " + address(c));
            count++;
         }
      }
      log.debug("count = " + count);
      assert count == 2;

      for (Cache<? super K, ?> c : caches()) {
         if (addresses.contains(address(c))) {
            assert c.getAdvancedCache().getDataContainer().containsKey(key);
         } else {
            assert !c.getAdvancedCache().getDataContainer().containsKey(key);
         }
      }
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(ConfigurationBuilder deConfiguration) {
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
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.transport().rackId(rack).machineId(machine);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(gcb, deConfiguration);
      cacheManagers.add(cm);
      return cm;
   }
}
