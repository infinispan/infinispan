package org.infinispan.distribution.topologyaware;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.topologyaware.TopologyAwareStateTransferTest")
@CleanupAfterTest
@InCacheMode({CacheMode.DIST_SYNC})
public class TopologyAwareStateTransferTest extends MultipleCacheManagersTest {

   private Address[] addresses;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(cacheMode);
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

   /**
    * dependsOnMethods does not work well with multiple instances of test.
    * See http://stackoverflow.com/questions/38345330 for details.
    */
   public void test() {
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

      EmbeddedCacheManager cm4 = cache(addresses[4]).getCacheManager();
      log.info("Here is where ST starts");
      TestingUtil.killCacheManagers(cm4);
      cacheManagers.remove(cm4);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForNoRebalance(caches());
      log.info("Here is where ST ends");
      List<Address> addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager()
                                                     .getWriteConsistentHash().getMembers();
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

      EmbeddedCacheManager cm2 = cache(addresses[2]).getCacheManager();
      TestingUtil.killCacheManagers(cm2);
      cacheManagers.remove(cm2);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForNoRebalance(caches());
      addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager()
                                                     .getWriteConsistentHash().getMembers();
      log.debug("After shutting down " + addresses[2] + " caches are " +  addressList);

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[1], TestingUtil.printCache(cache(addresses[1])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);

      EmbeddedCacheManager cm1 = cache(addresses[1]).getCacheManager();
      TestingUtil.killCacheManagers(cm1);
      cacheManagers.remove(cm1);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForNoRebalance(caches());
      addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager()
                                                     .getWriteConsistentHash().getMembers();
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
      LocalizedCacheTopology cacheTopology =
            cache(addresses[0]).getAdvancedCache().getDistributionManager().getCacheTopology();
      final List<Address> addresses = cacheTopology.getDistribution(key).writeOwners();
      log.debug(key + " should be present on = " + addresses);

      eventuallyEquals(2, () -> caches().stream().mapToInt(c -> c.getAdvancedCache().getDataContainer().containsKey(key) ? 1 : 0).sum());
      for (Cache<? super K, ?> c : caches()) {
         eventuallyEquals("Failure for key " + key + " on cache " + address(c), addresses.contains(address(c)),
               () -> c.getAdvancedCache().getDataContainer().containsKey(key));
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
