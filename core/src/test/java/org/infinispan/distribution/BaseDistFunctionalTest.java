package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.TopologyInfo;
import org.infinispan.distribution.ch.UnionConsistentHash;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.SECONDS;

@Test(groups = "functional", testName = "distribution.BaseDistFunctionalTest")
public abstract class BaseDistFunctionalTest extends MultipleCacheManagersTest {
   protected String cacheName;
   protected int INIT_CLUSTER_SIZE = 4;
   protected Cache<Object, String> c1 = null, c2 = null, c3 = null, c4 = null;
   protected Configuration configuration;
   protected List<Cache<Object, String>> caches;
   protected List<Address> cacheAddresses;
   protected boolean sync = true;
   protected boolean tx = false;
   protected boolean testRetVals = true;
   protected boolean l1CacheEnabled = true;
   protected boolean l1OnRehash = false;
   protected boolean performRehashing = false;
   protected boolean batchingEnabled = false;
   protected int numOwners = 2;
   protected int lockTimeout = 45;

   protected void createCacheManagers() throws Throwable {
      cacheName = "dist";
      configuration = getDefaultClusteredConfig(sync ? Configuration.CacheMode.DIST_SYNC : Configuration.CacheMode.DIST_ASYNC, tx);
      configuration.setRehashEnabled(performRehashing);
      configuration.setNumOwners(numOwners);
      if (!testRetVals) {
         configuration.setUnsafeUnreliableReturnValues(true);
         // we also need to use repeatable read for tests to work when we dont have reliable return values, since the
         // tests repeatedly queries changes
         configuration.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      }
      configuration.setInvocationBatchingEnabled(batchingEnabled);
      configuration.setSyncReplTimeout(60, TimeUnit.SECONDS);
      configuration.setLockAcquisitionTimeout(lockTimeout, TimeUnit.SECONDS);
      configuration.setL1CacheEnabled(l1CacheEnabled);
      if (l1CacheEnabled) configuration.setL1OnRehash(l1OnRehash);      
      caches = createClusteredCaches(INIT_CLUSTER_SIZE, cacheName, configuration);

      reorderBasedOnCHPositions();

      if (INIT_CLUSTER_SIZE > 0) c1 = caches.get(0);
      if (INIT_CLUSTER_SIZE > 1) c2 = caches.get(1);
      if (INIT_CLUSTER_SIZE > 2) c3 = caches.get(2);
      if (INIT_CLUSTER_SIZE > 3) c4 = caches.get(3);

      cacheAddresses = new ArrayList<Address>(INIT_CLUSTER_SIZE);
      for (Cache cache : caches) {
         EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) cache.getCacheManager();
         cacheAddresses.add(cacheManager.getAddress());
      }

      RehashWaiter.waitForInitRehashToComplete(caches.toArray(new Cache[INIT_CLUSTER_SIZE]));

   }

   public static ConsistentHash createNewConsistentHash(List<Address> servers) {
      try {
         Configuration c = new Configuration();
         c.setConsistentHashClass(DefaultConsistentHash.class.getName());
         return ConsistentHashHelper.createConsistentHash(c, servers, new TopologyInfo());
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * This is a separate class because some tools try and run this method as a test 
    */
   public static class RehashWaiter {
      private static final Log log = LogFactory.getLog(RehashWaiter.class);
      public static void waitForInitRehashToComplete(Cache... caches) {
         int gracetime = 60000; // 60 seconds?
         long giveup = System.currentTimeMillis() + gracetime;
         for (Cache c : caches) {
            DistributionManagerImpl dmi = (DistributionManagerImpl) TestingUtil.extractComponent(c, DistributionManager.class);
            while (!dmi.isJoinComplete()) {
               if (System.currentTimeMillis() > giveup) {
                  String message = "Timed out waiting for initial join sequence to complete on node " + dmi.rpcManager.getAddress() + " !";
                  log.error(message);
                  throw new RuntimeException(message);
               }
               LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
            log.trace("Node " + dmi.rpcManager.getAddress() + " finished rehash task.");
         }
      }

      public static void waitForRehashToComplete(Cache... caches) {
         int gracetime = 120000; // 120 seconds?
         long giveup = System.currentTimeMillis() + gracetime;
         for (Cache c : caches) {
            DistributionManagerImpl dmi = (DistributionManagerImpl) TestingUtil.extractComponent(c, DistributionManager.class);
            while (dmi.isRehashInProgress()) {
               if (System.currentTimeMillis() > giveup) {
                  String message = "Timed out waiting for rehash to complete on node " + dmi.rpcManager.getAddress() + " !";
                  log.error(message);
                  throw new RuntimeException(message);
               }
               LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
            }
            log.trace("Node " + dmi.rpcManager.getAddress() + " finished rehash task.");
         }
      }

      public static void waitForInitRehashToComplete(Collection<Cache> caches) {
         Set<Cache> cachesSet = new HashSet<Cache>();
         cachesSet.addAll(caches);
         waitForInitRehashToComplete(cachesSet.toArray(new Cache[cachesSet.size()]));
      }

      public static void waitForRehashToComplete(Collection<Cache> caches) {
         Set<Cache> cachesSet = new HashSet<Cache>();
         cachesSet.addAll(caches);
         waitForRehashToComplete(cachesSet.toArray(new Cache[cachesSet.size()]));
      }

   }

   // only used if the CH impl does not order the hash ring based on the order of the view.
   // in the case of the DefaultConsistentHash, the order is based on a has code of the addres modded by
   // the hash space.  So this will not adhere to the positions in the view, but it is deterministic.
   // so this function orders things such that the test can predict where keys get mapped to.
   private void reorderBasedOnCHPositions() {
      // wait for all joiners to join
      List<Cache> clist = new ArrayList<Cache>(cacheManagers.size());
      for (CacheContainer cm : cacheManagers) clist.add(cm.getCache(cacheName));
      assert clist.size() == INIT_CLUSTER_SIZE;
      waitForJoinTasksToComplete(SECONDS.toMillis(480), clist.toArray(new Cache[clist.size()]));

      // seed this with an initial cache.  Any one will do.
      Cache seed = caches.get(0);
      ConsistentHash ch = getNonUnionConsistentHash(seed, SECONDS.toMillis(480));
      List<Cache<Object, String>> reordered = new ArrayList<Cache<Object, String>>();

      
      for (Address a : ch.getCaches()) {
         for (Cache<Object, String> c : caches) {
            EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) c.getCacheManager();
            if (a.equals(cacheManager.getAddress())) {
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
            allOK &= dmi.isJoinComplete();
         }
         if (allOK) return;
         TestingUtil.sleepThread(100);
      }
      throw new RuntimeException("Some caches have not finished rehashing after " + Util.prettyPrintTime(timeout));
   }


   protected void initAndTest() {
      for (Cache<Object, String> c : caches) assert c.isEmpty();

      c1.put("k1", "value");
      asyncWait("k1", PutKeyValueCommand.class, getNonOwnersExcludingSelf("k1", addressOf(c1)));
      for (Cache<Object, String> c : caches)
         assert "value".equals(c.get("k1")) : "Failed on cache " + addressOf(c);
      assertOwnershipAndNonOwnership("k1");
   }

   protected Address addressOf(Cache<?, ?> cache) {
      return DistributionTestHelper.addressOf(cache);
   }

   protected Cache<Object, String> getFirstNonOwner(String key) {
      return getNonOwners(key)[0];
   }
   
   protected Cache<Object, String> getFirstOwner(String key) {
      return getOwners(key)[0];
   }

   protected Cache<Object, String> getSecondNonOwner(String key) {
      return getNonOwners(key)[1];
   }

   protected void assertOnAllCachesAndOwnership(Object key, String value) {
      assertOnAllCaches(key, value);
      if (value != null) assertOwnershipAndNonOwnership(key);
   }

   protected void assertOnAllCaches(Object key, String value) {
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
   }

   protected void assertOwnershipAndNonOwnership(Object key) {
      for (Cache<Object, String> c : caches) {
         DataContainer dc = c.getAdvancedCache().getDataContainer();
         InternalCacheEntry ice = dc.get(key);
         if (isOwner(c, key)) {
            assert ice != null : "Fail on cache " + addressOf(c) + ": dc.get(" + key + ") returned null!";
            assert ice instanceof ImmortalCacheEntry : "Fail on cache " + addressOf(c) + ": dc.get(" + key + ") returned " + safeType(ice);
         }
         // Invalidation may need some time to "catch up", so this should not be strictly enforced if the node is a NON OWNER.
      }
   }

   protected int locateJoiner(Address joinerAddress) {
      for (Cache c : caches) {
         ConsistentHash dch = getNonUnionConsistentHash(c, SECONDS.toMillis(480));
         int i = 0;
         for (Address a : dch.getCaches()) {
            if (a.equals(joinerAddress)) return i;
            i++;
         }
      }
      throw new RuntimeException("Cannot locate joiner! Joiner is [" + joinerAddress + "]");
   }

   protected String safeType(Object o) {
      return DistributionTestHelper.safeType(o);
   }

   protected void assertIsInL1(Cache<?, ?> cache, Object key) {
      DistributionTestHelper.assertIsInL1(cache, key);
   }

   protected void assertIsNotInL1(Cache<?, ?> cache, Object key) {
      DistributionTestHelper.assertIsNotInL1(cache, key);
   }

   protected void assertIsInContainerImmortal(Cache<?, ?> cache, Object key) {
      DistributionTestHelper.assertIsInContainerImmortal(cache, key);
   }

   protected void assertIsInL1OrNull(Cache<?, ?> cache, Object key) {
      DistributionTestHelper.assertIsInL1OrNull(cache, key);
   }

   protected boolean isOwner(Cache<?, ?> c, Object key) {
      return DistributionTestHelper.isOwner(c, key);
   }

   protected boolean isFirstOwner(Cache<?, ?> c, Object key) {
      return DistributionTestHelper.isFirstOwner(c, key);
   }

   public Cache<Object, String>[] getOwners(Object key) {
      return getOwners(key, 2);
   }

   public Cache<Object, String>[] getOwners(Object key, int expectedNumberOwners) {
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
      return getNonOwners(key, 2);
   }

   protected Cache<Object, String>[] getNonOwners(Object key, int expectedNumberNonOwners) {
      Cache<Object, String>[] nonOwners = new Cache[expectedNumberNonOwners];
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

   protected ConsistentHash getConsistentHash(Cache<?, ?> c) {
      return getDistributionManager(c).getConsistentHash();
   }

   protected ConsistentHash getNonUnionConsistentHash(Cache<?, ?> c, long timeout) {
      long expTime = System.currentTimeMillis() + timeout;
      while (System.currentTimeMillis() < expTime) {
         ConsistentHash ch = getDistributionManager(c).getConsistentHash();
         if (!(ch instanceof UnionConsistentHash)) return ch;
         TestingUtil.sleepThread(100);
      }
      throw new RuntimeException("Timed out waiting for a non-UnionConsistentHash to be present on cache [" + addressOf(c) + "]");
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
}
