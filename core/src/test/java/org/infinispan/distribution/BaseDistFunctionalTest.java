package org.infinispan.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.L1InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.groups.KXGrouper;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.concurrent.IsolationLevel;


public abstract class BaseDistFunctionalTest<K, V> extends MultipleCacheManagersTest {
   protected String cacheName;
   protected int INIT_CLUSTER_SIZE = 4;
   protected Cache<K, V> c1 = null, c2 = null, c3 = null, c4 = null;
   protected ConfigurationBuilder configuration;
   protected List<Cache<K, V>> caches;
   protected List<Address> cacheAddresses;
   protected boolean testRetVals = true;
   protected boolean l1CacheEnabled = true;
   protected int l1Threshold = 5;
   protected boolean performRehashing = false;
   protected boolean batchingEnabled = false;
   protected int numOwners = 2;
   protected int lockTimeout = 45;
   protected boolean groupers = false;
   protected boolean onePhaseCommitOptimization = false;

   {
      cacheMode = CacheMode.DIST_SYNC;
      transactional = false;
   }

   public BaseDistFunctionalTest numOwners(int numOwners) {
      this.numOwners = numOwners;
      return this;
   }

   public BaseDistFunctionalTest l1(boolean l1) {
      this.l1CacheEnabled = l1;
      return this;
   }

   public BaseDistFunctionalTest groupers(boolean groupers) {
      this.groupers = groupers;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "numOwners", "l1", "groupers");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), numOwners != 2 ? numOwners : null, l1CacheEnabled ? null : Boolean.FALSE, groupers ? Boolean.TRUE : null);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      cacheName = "dist";
      configuration = buildConfiguration();
      // Create clustered caches with failure detection protocols on
      caches = createClusteredCaches(INIT_CLUSTER_SIZE, cacheName, configuration,
                                     new TransportFlags().withFD(false));

      if (INIT_CLUSTER_SIZE > 0) c1 = caches.get(0);
      if (INIT_CLUSTER_SIZE > 1) c2 = caches.get(1);
      if (INIT_CLUSTER_SIZE > 2) c3 = caches.get(2);
      if (INIT_CLUSTER_SIZE > 3) c4 = caches.get(3);

      cacheAddresses = new ArrayList<Address>(INIT_CLUSTER_SIZE);
      for (Cache cache : caches) {
         EmbeddedCacheManager cacheManager = cache.getCacheManager();
         cacheAddresses.add(cacheManager.getAddress());
      }
   }

   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(cacheMode, transactional);
      configuration.clustering().stateTransfer().fetchInMemoryState(performRehashing);
      if (lockingMode != null) {
         configuration.transaction().lockingMode(lockingMode);
      }
      configuration.clustering().hash().numOwners(numOwners);
      if (!testRetVals) {
         configuration.unsafe().unreliableReturnValues(true);
         // we also need to use repeatable read for tests to work when we dont have reliable return values, since the
         // tests repeatedly queries changes
         configuration.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      }
      if (transactional) {
         configuration.invocationBatching().enable();
         if (onePhaseCommitOptimization) {
            configuration.transaction().use1PcForAutoCommitTransactions(true);
         }
      }
      if (cacheMode.isSynchronous()) configuration.clustering().remoteTimeout(60, TimeUnit.SECONDS);
      configuration.locking().lockAcquisitionTimeout(lockTimeout, TimeUnit.SECONDS);
      configuration.clustering().l1().enabled(l1CacheEnabled);
      if (groupers) {
          configuration.clustering().hash().groups().enabled(true);
          configuration.clustering().hash().groups().withGroupers(Collections.singletonList(new KXGrouper()));
      }
      if (l1CacheEnabled) configuration.clustering().l1().invalidationThreshold(l1Threshold);
      return configuration;
   }

   // ----------------- HELPERS ----------------

   protected boolean isTriangle() {
      return TestingUtil.isTriangleAlgorithm(cacheMode, transactional);
   }

   protected void initAndTest() {
      for (Cache<K, V> c : caches) assert c.isEmpty();

      // TODO: A bit hacky, this should be moved somewhere else really...
      Cache<Object, Object> firstCache = (Cache<Object, Object>) caches.get(0);
      firstCache.put("k1", "value");
      asyncWait("k1", PutKeyValueCommand.class);
      assertOnAllCachesAndOwnership("k1", "value");
   }

   protected Address addressOf(Cache<?, ?> cache) {
      return DistributionTestHelper.addressOf(cache);
   }

   protected Cache<K, V> getFirstNonOwner(Object key) {
      return DistributionTestHelper.getFirstNonOwner(key, caches);
   }

   protected Cache<K, V> getFirstOwner(Object key) {
      return DistributionTestHelper.getFirstOwner(key, caches);
   }

   protected Cache<K, V> getSecondNonOwner(String key) {
      return getNonOwners(key)[1];
   }

   protected void assertOnAllCachesAndOwnership(Object key, String value) {
      assertOwnershipAndNonOwnership(key, l1CacheEnabled);
      // checking the values will bring the keys to L1, so we want to do it after checking ownership
      assertOnAllCaches(key, value);
   }

   protected void assertRemovedOnAllCaches(Object key) {
      assertOnAllCaches(key, null);
   }

   protected void assertOnAllCaches(Object key, String value) {
      for (Cache<K, V> c : caches) {
         Object realVal = c.get(key);
         if (value == null) {
            assert realVal == null : "Expecting [" + key + "] to equal [" + value + "] on cache ["
                  + addressOf(c) + "] but was [" + realVal + "]. Owners are " + Arrays.toString(getOwners(key));
         } else {
            assert value.equals(realVal) : "Expecting [" + key + "] to equal [" + value + "] on cache ["
                  + addressOf(c) + "] but was [" + realVal + "]";
         }
      }
      // Allow some time for all ClusteredGetCommands to finish executing
      TestingUtil.sleepThread(100);
   }

   protected void assertOwnershipAndNonOwnership(Object key, boolean allowL1) {
      for (Cache<K, V> c : caches) {
         DataContainer dc = c.getAdvancedCache().getDataContainer();
         InternalCacheEntry ice = dc.get(key);
         if (isOwner(c, key)) {
            assert ice != null : "Fail on owner cache " + addressOf(c) + ": dc.get(" + key + ") returned null!";
            assert ice instanceof ImmortalCacheEntry : "Fail on owner cache " + addressOf(c) + ": dc.get(" + key + ") returned " + safeType(ice);
         } else {
            if (allowL1) {
               assert ice == null || ice instanceof L1InternalCacheEntry : "Fail on non-owner cache " + addressOf(c) + ": dc.get(" + key + ") returned " + safeType(ice);
            } else {
               // Segments no longer owned are invalidated asynchronously
               eventuallyEquals("Fail on non-owner cache " + addressOf(c) + ": dc.get(" + key + ")",
                     null, () -> dc.get(key));
            }
         }
      }
   }

   protected String safeType(Object o) {
      return DistributionTestHelper.safeType(o);
   }

   protected boolean isInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      return ice != null && !(ice instanceof ImmortalCacheEntry);
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

   protected Cache<K, V>[] getOwners(Object key) {
      Cache<K, V>[] arr = new Cache[numOwners];
      DistributionTestHelper.getOwners(key, caches).toArray(arr);
      return arr;
   }

   protected Cache<K, V>[] getOwners(Object key, int expectedNumberOwners) {
      Cache<K, V>[] arr = new Cache[expectedNumberOwners];
      DistributionTestHelper.getOwners(key, caches).toArray(arr);
      return arr;
   }

   protected Cache<K, V>[] getNonOwnersExcludingSelf(Object key, Address self) {
      Cache<K, V>[] nonOwners = getNonOwners(key);
      boolean selfInArray = false;
      for (Cache<?, ?> c : nonOwners) {
         if (addressOf(c).equals(self)) {
            selfInArray = true;
            break;
         }
      }

      if (selfInArray) {
         Cache<K, V>[] nonOwnersExclSelf = new Cache[nonOwners.length - 1];
         int i = 0;
         for (Cache<K, V> c : nonOwners) {
            if (!addressOf(c).equals(self)) nonOwnersExclSelf[i++] = c;
         }
         return nonOwnersExclSelf;
      } else {
         return nonOwners;
      }
   }

   protected Cache<K, V>[] getNonOwners(Object key) {
      return getNonOwners(key, 2);
   }

   protected Cache<K, V>[] getNonOwners(Object key, int expectedNumberNonOwners) {
      Cache<K, V>[] nonOwners = new Cache[expectedNumberNonOwners];
      DistributionTestHelper.getNonOwners(key, caches).toArray(nonOwners);
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

   protected TransactionManager getTransactionManager(Cache<?, ?> cache) {
      return TestingUtil.getTransactionManager(cache);
   }

   protected static void removeAllBlockingInterceptorsFromCache(Cache<?, ?> cache) {
      AsyncInterceptorChain chain = cache.getAdvancedCache().getAsyncInterceptorChain();
      BlockingInterceptor blockingInterceptor = chain.findInterceptorExtending(BlockingInterceptor.class);
      while (blockingInterceptor != null) {
         blockingInterceptor.suspend(true);
         chain.removeInterceptor(blockingInterceptor.getClass());
         blockingInterceptor = chain.findInterceptorExtending(BlockingInterceptor.class);
      }
   }
}
