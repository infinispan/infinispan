package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import static org.infinispan.config.Configuration.CacheMode.DIST_ASYNC;
import static org.infinispan.config.Configuration.CacheMode.DIST_SYNC;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "functional", testName = "distribution.BaseDistFunctionalTest")
public abstract class BaseDistFunctionalTest extends MultipleCacheManagersTest {
   protected Cache<Object, String> c1, c2, c3, c4;
   protected List<Cache<Object, String>> caches;
   protected boolean sync = true;
   protected boolean tx = false;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(sync ? DIST_SYNC : DIST_ASYNC);
      if (tx) c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      caches = createClusteredCaches(4, "dist", c);
      c1 = caches.get(0);
      c2 = caches.get(0);
      c3 = caches.get(0);
      c4 = caches.get(0);
   }

   // ----------------- HELPERS ----------------

   protected void initAndTest() {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      c1.put("k1", "value");
      asyncWait();
      for (Cache<Object, String> c : caches) assert c.get("k1").equals("value");
      assertOwnershipAndNonOwnership("k1");
   }

   protected Cache<Object, String> getFirstNonOwner(String key) {
      return getNonOwners(key).get(0);
   }

   protected void assertOnAllCachesAndOwnership(Object key, String value) {
      for (Cache<Object, String> c : caches) {
         if (value == null)
            assert c.get(key) == null;
         else
            assert value.equals(c.get(key));
      }
      assertOwnershipAndNonOwnership("k1");
   }

   protected void assertOwnershipAndNonOwnership(Object key) {
      for (Cache<Object, String> c : caches) {
         DataContainer dc = c.getAdvancedCache().getDataContainer();
         if (isOwner(c, key)) {
            assert dc.get(key) instanceof ImmortalCacheEntry : "Fail on cache " + c.getCacheManager().getAddress() + ": dc.get(" + key + ") returned " + dc.get(key);
         } else {
            if (dc.containsKey(key)) {
               assert dc.get(key) instanceof MortalCacheEntry : "Fail on cache " + c.getCacheManager().getAddress() + ": dc.get(" + key + ") returned " + dc.get(key);
               assert dc.get(key).getLifespan() == c1.getConfiguration().getL1Lifespan();
            }
         }
      }
   }

   protected void assertIsInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice != null : "Entry for key [" + key + "] should be in data container on cache at [" + cache.getCacheManager().getAddress() + "]!";
      assert !(ice instanceof ImmortalCacheEntry) : "Entry for key [" + key + "] should have a lifespan on cache at [" + cache.getCacheManager().getAddress() + "]!";
   }

   protected void assertIsNotInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice == null : "Entry for key [" + key + "] should not be in data container on cache at [" + cache.getCacheManager().getAddress() + "]!";
   }

   protected void assertIsInContainerImmortal(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice != null : "Entry for key [" + key + "] should be in data container on cache at [" + cache.getCacheManager().getAddress() + "]!";
      assert ice instanceof ImmortalCacheEntry : "Entry for key [" + key + "] on cache at [" + cache.getCacheManager().getAddress() + "] should be immortal but was [" + ice + "]!";
   }

   protected static boolean isOwner(Cache<?, ?> c, Object key) {
      DistributionManager dm = c.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class);
      List<Address> ownerAddresses = dm.locate(key);
      for (Address a : ownerAddresses) {
         if (c.getCacheManager().getAddress().equals(a)) return true;
      }
      return false;
   }

   protected List<Cache<Object, String>> getOwners(Object key) {
      List<Cache<Object, String>> owners = new ArrayList<Cache<Object, String>>();
      for (Cache<Object, String> c : caches) {
         if (isOwner(c, key)) owners.add(c);
      }
      return owners;
   }

   protected List<Cache<Object, String>> getNonOwners(Object key) {
      List<Cache<Object, String>> nonOwners = new ArrayList<Cache<Object, String>>();
      for (Cache<Object, String> c : caches) {
         if (!isOwner(c, key)) nonOwners.add(c);
      }
      return nonOwners;
   }

   protected List<Address> residentAddresses(Object key) {
      DistributionManager dm = c1.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class);
      return dm.locate(key);
   }

   protected void asyncWait() {
      // no op.
   }

   protected TransactionManager getTransactionManager(Cache<?, ?> cache) {
      return TestingUtil.getTransactionManager(cache);
   }

   /**
    * A special type of key that if passed a cache in its constructor, will ensure it will always be assigned to that
    * cache (plus however many additional caches in the hash space)
    */
   protected static class MagicKey implements Serializable {
      int hashcode;
      String address;

      public MagicKey(Cache<?, ?> toMapTo) {
         address = toMapTo.getCacheManager().getAddress().toString();
         // generate a hashcode that will always map it to the specified cache.
         for (int i = 0; i < DefaultConsistentHash.HASH_SPACE; i += 100) {
            // create a dummy object with this hashcode
            final int hc = i;
            Object dummy = new Object() {
               @Override
               public int hashCode() {
                  return hc;
               }
            };

            if (BaseDistFunctionalTest.isOwner(toMapTo, dummy)) {
               // we have found a hashcode that works!
               hashcode = hc;
               break;
            }
         }
      }

      @Override
      public int hashCode() {
         return hashcode;
      }

      @Override
      public String toString() {
         return "MagicKey{" +
               "hashcode=" + hashcode +
               ", address='" + address + '\'' +
               '}';
      }
   }
}
