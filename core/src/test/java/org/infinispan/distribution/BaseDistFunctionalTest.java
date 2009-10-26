package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.manager.CacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.concurrent.locks.LockSupport;

@Test(groups = "functional", testName = "distribution.BaseDistFunctionalTest")
public abstract class BaseDistFunctionalTest extends MultipleCacheManagersTest {
   protected String cacheName;
   protected Cache<Object, String> c1, c2, c3, c4;
   protected Configuration configuration;
   protected List<Cache<Object, String>> caches;
   protected List<Address> cacheAddresses;
   protected boolean sync = true;
   protected boolean tx = false;
   protected boolean testRetVals = true;

   protected void createCacheManagers() throws Throwable {
      cacheName = "dist";
      configuration = getDefaultClusteredConfig(sync ? Configuration.CacheMode.DIST_SYNC : Configuration.CacheMode.DIST_ASYNC, tx);
      if (!testRetVals) {
         configuration.setUnsafeUnreliableReturnValues(true);
         // we also need to use repeatable read for tests to work when we dont have reliable return values, since the
         // tests repeatedly queries changes
         configuration.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      }
      configuration.setSyncReplTimeout(60, TimeUnit.SECONDS);
      configuration.setLockAcquisitionTimeout(45, TimeUnit.SECONDS);
      caches = createClusteredCaches(4, cacheName, configuration);

      reorderBasedOnCHPositions();

      c1 = caches.get(0);
      c2 = caches.get(1);
      c3 = caches.get(2);
      c4 = caches.get(3);

      cacheAddresses = new ArrayList<Address>(4);
      for (Cache cache : caches) cacheAddresses.add(cache.getCacheManager().getAddress());

      RehashWaiter.waitForInitRehashToComplete(c1, c2, c3, c4);

   }
   
   /**
    * This is a separate class because some tools try and run this method as a test 
    */
   public static class RehashWaiter {
      public static void waitForInitRehashToComplete(Cache... caches) {
         int gracetime = 60000; // 60 seconds?
         long giveup = System.currentTimeMillis() + gracetime;
         for (Cache c : caches) {
            DistributionManagerImpl dmi = (DistributionManagerImpl) TestingUtil.extractComponent(c, DistributionManager.class);
            while (!dmi.joinComplete) {
               if (System.currentTimeMillis() > giveup)
                  throw new RuntimeException("Timed out waiting for initial join sequence to complete!");
               LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
         }
      }
   }

   // only used if the CH impl does not order the hash ring based on the order of the view.
   // in the case of the DefaultConsistentHash, the order is based on a has code of the addres modded by
   // the hash space.  So this will not adhere to the positions in the view, but it is deterministic.
   // so this function orders things such that the test can predict where keys get mapped to.
   private void reorderBasedOnCHPositions() {
      // wait for all joiners to join
      List<Cache> clist = new ArrayList<Cache>(cacheManagers.size());
      for (CacheManager cm : cacheManagers) clist.add(cm.getCache(cacheName));
      assert clist.size() == 4;
      waitForJoinTasksToComplete(SECONDS.toMillis(480), clist.toArray(new Cache[clist.size()]));

      // seed this with an initial cache.  Any one will do.
      Cache seed = caches.get(0);
      DefaultConsistentHash ch = getDefaultConsistentHash(seed, SECONDS.toMillis(480));
      List<Cache<Object, String>> reordered = new ArrayList<Cache<Object, String>>();

      
      for (Address a : ch.getCaches()) {
         for (Cache<Object, String> c : caches) {
            if (a.equals(c.getCacheManager().getAddress())) {
               reordered.add(c);
               break;
            }
         }
      }

      caches = reordered;
   }

   // ----------------- HELPERS ----------------

   protected void waitForJoinTasksToComplete(long timeout, Cache... joiners) {
      long giveupTime = System.currentTimeMillis() + timeout;
      while (System.currentTimeMillis() < giveupTime) {
         boolean allOK = true;
         for (Cache c : joiners) {
            DistributionManagerImpl dmi = (DistributionManagerImpl) getDistributionManager(c);
            allOK &= dmi.joinComplete;
         }
         if (allOK) return;
         TestingUtil.sleepThread(100);
      }
      throw new RuntimeException("Some caches have not finished rehashing after " + Util.prettyPrintTime(timeout));
   }


   protected void initAndTest() {
      System.out.println("Caches are " + cacheAddresses);

      for (Cache<Object, String> c : caches) assert c.isEmpty();

      c1.put("k1", "value");
      asyncWait("k1", PutKeyValueCommand.class, getNonOwnersExcludingSelf("k1", addressOf(c1)));
      for (Cache<Object, String> c : caches)
         assert "value".equals(c.get("k1")) : "Failed on cache " + addressOf(c);
      assertOwnershipAndNonOwnership("k1");
   }

   protected static Address addressOf(Cache<?, ?> cache) {
      return cache.getCacheManager().getAddress();
   }

   protected Cache<Object, String> getFirstNonOwner(String key) {
      return getNonOwners(key)[0];
   }

   protected Cache<Object, String> getSecondNonOwner(String key) {
      return getNonOwners(key)[1];
   }

   protected void assertOnAllCachesAndOwnership(Object key, String value) {
      for (Cache<Object, String> c : caches) {
         Object realVal = c.get(key);
         if (value == null) {
            assert realVal == null : "Expecting [" + key + "] to equal [" + value + "] on cache ["
                  + addressOf(c) + "] but was [" + realVal + "]";
         } else {
            assert value.equals(realVal) : "Expecting [" + key + "] to equal [" + value + "] on cache ["
                  + addressOf(c) + "] but was [" + realVal + "]";
         }
      }
      if (value != null) assertOwnershipAndNonOwnership(key);
   }

   protected void assertOwnershipAndNonOwnership(Object key) {
      for (Cache<Object, String> c : caches) {
         DataContainer dc = c.getAdvancedCache().getDataContainer();
         if (isOwner(c, key)) {
            InternalCacheEntry ice = dc.get(key);
            assert ice != null : "Fail on cache " + addressOf(c) + ": dc.get(" + key + ") returned null!";
            assert ice instanceof ImmortalCacheEntry : "Fail on cache " + addressOf(c) + ": dc.get(" + key + ") returned " + safeType(dc.get(key));
         } else {
            if (dc.containsKey(key)) {
               assert dc.get(key) instanceof MortalCacheEntry : "Fail on cache " + addressOf(c) + ": dc.get(" + key + ") returned " + safeType(dc.get(key));
               assert dc.get(key).getLifespan() == c1.getConfiguration().getL1Lifespan();
            }
         }
      }
   }

   protected static final String safeType(Object o) {
      if (o == null) return "null";
      return o.getClass().getSimpleName();
   }

   protected void assertIsInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice != null : "Entry for key [" + key + "] should be in data container on cache at [" + addressOf(cache) + "]!";
      assert !(ice instanceof ImmortalCacheEntry) : "Entry for key [" + key + "] should have a lifespan on cache at [" + addressOf(cache) + "]!";
   }

   protected void assertIsNotInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice == null : "Entry for key [" + key + "] should not be in data container on cache at [" + addressOf(cache) + "]!";
   }

   protected void assertIsInContainerImmortal(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice != null : "Entry for key [" + key + "] should be in data container on cache at [" + addressOf(cache) + "]!";
      assert ice instanceof ImmortalCacheEntry : "Entry for key [" + key + "] on cache at [" + addressOf(cache) + "] should be immortal but was [" + ice + "]!";
   }

   protected static boolean isOwner(Cache<?, ?> c, Object key) {
      DistributionManager dm = c.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class);
      List<Address> ownerAddresses = dm.locate(key);
      for (Address a : ownerAddresses) {
         if (addressOf(c).equals(a)) return true;
      }
      return false;
   }

   protected static boolean isFirstOwner(Cache<?, ?> c, Object key) {
      DistributionManager dm = c.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class);
      List<Address> ownerAddresses = dm.locate(key);
      return addressOf(c).equals(ownerAddresses.get(0));
   }

   protected Cache<Object, String>[] getOwners(Object key) {
      return getOwners(key, 2);
   }

   protected Cache<Object, String>[] getOwners(Object key, int expectedNumberOwners) {
      Cache<Object, String>[] owners = new Cache[expectedNumberOwners];
      int i = 0;
      for (Cache<Object, String> c : caches) {
         if (isOwner(c, key)) owners[i++] = c;
      }
      for (Cache<?, ?> c : owners) assert c != null : "Have not found enough owners for key [" + key + "]";
      return owners;
   }

   protected Cache<Object, String>[] getNonOwnersExcludingSelf(Object key, Address self) {
      Cache<Object, String>[] nonOwners = getNonOwners(key);
      boolean selfInArray = false;
      for (Cache<?, ?> c : nonOwners) {
         if (addressOf(c).equals(self)) {
            selfInArray = true;
            break;
         }
      }

      if (selfInArray) {
         Cache<Object, String>[] nonOwnersExclSelf = new Cache[nonOwners.length - 1];
         int i = 0;
         for (Cache<Object, String> c : nonOwners) {
            if (!addressOf(c).equals(self)) nonOwnersExclSelf[i++] = c;
         }
         return nonOwnersExclSelf;
      } else {
         return nonOwners;
      }
   }

   protected Cache<Object, String>[] getNonOwners(Object key) {
      Cache<Object, String>[] nonOwners = new Cache[2];
      int i = 0;
      for (Cache<Object, String> c : caches) {
         if (!isOwner(c, key)) nonOwners[i++] = c;
      }
      return nonOwners;
   }

   protected List<Address> residentAddresses(Object key) {
      DistributionManager dm = c1.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class);
      return dm.locate(key);
   }

   protected DistributionManager getDistributionManager(Cache<?, ?> c) {
      return c.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class);
   }

   protected DefaultConsistentHash getDefaultConsistentHash(Cache<?, ?> c) {
      return (DefaultConsistentHash) getDistributionManager(c).getConsistentHash();
   }

   protected DefaultConsistentHash getDefaultConsistentHash(Cache<?, ?> c, long timeout) {
      long expTime = System.currentTimeMillis() + timeout;
      while (System.currentTimeMillis() < expTime) {
         ConsistentHash ch = getDistributionManager(c).getConsistentHash();
         if (ch instanceof DefaultConsistentHash) return (DefaultConsistentHash) ch;
         TestingUtil.sleepThread(100);
      }
      throw new RuntimeException("Timed out waiting for a DefaultConsistentHash to be present on cache [" + addressOf(c) + "]");
   }

   /**
    * Blocks and waits for a replication event on async caches
    *
    * @param key     key that causes the replication.  Used to determine which caches to listen on.  If null, all caches
    *                are checked
    * @param command command to listen for
    * @param caches  on which this key should be invalidated
    */
   protected void asyncWait(Object key, Class<? extends VisitableCommand> command, Cache<?, ?>... caches) {
      // no op.
   }

   protected void assertProperConsistentHashOnAllCaches() {
      // check that ALL caches in the system DON'T have a temporary UnionCH
      for (Cache c : caches) {
         DistributionManager dm = getDistributionManager(c);
         assert !(dm.getConsistentHash() instanceof UnionConsistentHash);
      }
   }

   protected TransactionManager getTransactionManager(Cache<?, ?> cache) {
      return TestingUtil.getTransactionManager(cache);
   }

   /**
    * A special type of key that if passed a cache in its constructor, will ensure it will always be assigned to that
    * cache (plus however many additional caches in the hash space)
    */
   public static class MagicKey implements Serializable {
      String name = null;
      int hashcode;
      String address;

      public MagicKey(Cache<?, ?> toMapTo) {
         address = addressOf(toMapTo).toString();
         Random r = new Random();
         for (; ;) {
            // create a dummy object with this hashcode
            final int hc = r.nextInt();
            Object dummy = new Object() {
               @Override
               public int hashCode() {
                  return hc;
               }
            };

            if (BaseDistFunctionalTest.isFirstOwner(toMapTo, dummy)) {
               // we have found a hashcode that works!
               hashcode = hc;
               break;
            }
         }
      }

      public MagicKey(Cache<?, ?> toMapTo, String name) {
         this(toMapTo);
         this.name = name;
      }

      @Override
      public int hashCode() {
         return hashcode;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         MagicKey magicKey = (MagicKey) o;

         if (hashcode != magicKey.hashcode) return false;
         if (address != null ? !address.equals(magicKey.address) : magicKey.address != null) return false;

         return true;
      }

      @Override
      public String toString() {
         return "MagicKey{" +
               (name == null ? "" : "name=" + name + ", ") +
               "hashcode=" + hashcode +
               ", address='" + address + '\'' +
               '}';
      }
   }
}
