package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

/**
 * Test helper class
 *
 * @author Manik Surtani
 * @since 4.2.1
 */
public class DistributionTestHelper {
   public static String safeType(Object o) {
      if (o == null) return "null";
      return o.getClass().getSimpleName();
   }

   public static void assertIsInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice != null : "Entry for key [" + key + "] should be in L1 on cache at [" + addressOf(cache) + "]!";
      assert !(ice instanceof ImmortalCacheEntry) : "Entry for key [" + key + "] should have a lifespan on cache at [" + addressOf(cache) + "]!";
   }

   public static void assertIsNotInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice == null : "Entry for key [" + key + "] should not be in data container at all on cache at [" + addressOf(cache) + "]!";
   }

   public static void assertIsInContainerImmortal(Cache<?, ?> cache, Object key) {
      Log log = LogFactory.getLog(BaseDistFunctionalTest.class);
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      if (ice == null) {
         String msg = "Entry for key [" + key + "] should be in data container on cache at [" + addressOf(cache) + "]!";
         log.fatal(msg);
         assert false : msg;
      }

      if (!(ice instanceof ImmortalCacheEntry)) {
         String msg = "Entry for key [" + key + "] on cache at [" + addressOf(cache) + "] should be immortal but was [" + ice + "]!";
         log.fatal(msg);
         assert false : msg;
      }
   }

   public static void assertIsInL1OrNull(Cache<?, ?> cache, Object key) {
      Log log = LogFactory.getLog(BaseDistFunctionalTest.class);
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      if (ice instanceof ImmortalCacheEntry) {
         String msg = "Entry for key [" + key + "] on cache at [" + addressOf(cache) + "] should be mortal or null but was [" + ice + "]!";
         log.fatal(msg);
         assert false : msg;
      }
   }


   public static boolean isOwner(Cache<?, ?> c, Object key) {
      DistributionManager dm = c.getAdvancedCache().getDistributionManager();
      List<Address> ownerAddresses = dm.locate(key);
      for (Address a : ownerAddresses) {
         if (addressOf(c).equals(a)) return true;
      }
      return false;
   }

   public static boolean isFirstOwner(Cache<?, ?> c, Object key) {
      DistributionManager dm = c.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class);
      List<Address> ownerAddresses = dm.locate(key);
      return addressOf(c).equals(ownerAddresses.get(0));
   }

   public static Address addressOf(Cache<?, ?> cache) {
      EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) cache.getCacheManager();
      return cacheManager.getAddress();

   }
}
