package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LookupMode;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Test that entries in a shared store are not touched in any way during state transfer.
 *
 * @author Dan Berindei
 */
@Test(testName = "distribution.rehash.SharedStoreInvalidationDuringRehashTest", groups = "functional")
@CleanupAfterMethod
public class SharedStoreInvalidationDuringRehashTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(SharedStoreInvalidationDuringRehashTest.class);

   private static final int NUM_KEYS = 20;
   private static final String TEST_CACHE_NAME = "testCache";

   private final Map<Integer, AtomicInteger> invalidationCounts = CollectionFactory.makeConcurrentMap();
   private final Map<Integer, AtomicInteger> l1InvalidationCounts = CollectionFactory.makeConcurrentMap();

   @Override
   protected void createCacheManagers() {
      // cacheManagers started one after another in test()
   }

   private void addNewCacheManagerAndWaitForRehash(int index, boolean preload) {
      EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(getDefaultClusteredCacheConfig(
            CacheMode.DIST_SYNC, false));
      Configuration config = buildCfg(index, true, preload);
      cacheManager.defineConfiguration(TEST_CACHE_NAME, config);
      log.debugf("\n\nstarted CacheManager #%d", getCacheManagers().size() - 1);
      waitForClusterToForm(TEST_CACHE_NAME);
   }

   private Configuration buildCfg(final int index, boolean clustered, boolean preload) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.persistence().passivation(false);
      cb.customInterceptors().addInterceptor().index(0).interceptor(new BaseCustomInterceptor() {
         @Override
         public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
            incrementCounter(invalidationCounts, index, invalidateCommand.getKeys().length);
            return invokeNextInterceptor(ctx, invalidateCommand);
         }

         @Override
         public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command) throws Throwable {
            incrementCounter(l1InvalidationCounts, index, invalidateL1Command.getKeys().length);
            return invokeNextInterceptor(ctx, invalidateL1Command);
         }
      });

      DummyInMemoryStoreConfigurationBuilder dummyCB = cb.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      dummyCB.debug(true).preload(preload).shared(true).purgeOnStartup(false);
      dummyCB.storeName(SharedStoreInvalidationDuringRehashTest.class.getSimpleName());

      if (clustered) {
         cb.clustering().l1().disable();
         cb.clustering().cacheMode(CacheMode.DIST_SYNC);
         cb.clustering().hash().numOwners(1); // one owner!

         cb.clustering().stateTransfer().fetchInMemoryState(true);
         cb.clustering().hash().groups().enabled();
      }
      return cb.build(true);
   }

   private void incrementCounter(Map<Integer, AtomicInteger> counterMap, int index, int delta) {
      AtomicInteger counter = counterMap.get(index);
      if (counter == null) {
         counter = new AtomicInteger(0);
         counterMap.put(index, counter);
      }
      counter.addAndGet(delta);
   }

   private int getCounter(Map<Integer, AtomicInteger> counterMap, int index) {
      AtomicInteger counter = counterMap.get(index);
      return counter != null ? counter.get() : 0;
   }

   private int getSum(Map<Integer, AtomicInteger> counterMap) {
      int sum = 0;
      for (AtomicInteger c : counterMap.values()) {
         sum += c.get();
      }
      return sum;
   }

   public void testRehashWithPreload() {
      doTest(true);
   }

   public void testRehashWithoutPreload() {
      doTest(false);
   }

   private void doTest(boolean preload) {

      // start a cluster that uses this cache store
      // add 1st member
      addNewCacheManagerAndWaitForRehash(0, preload);

      // insert the data and test that it's in the store
      insertTestData();
      printCacheContents();
      printStoreContents();
      checkContentAndInvalidations(preload);

      // stop 1st member
      killMember(0);

      // re-add 1st member
      addNewCacheManagerAndWaitForRehash(0, preload);
      printCacheContents();
      printStoreContents();
      checkContentAndInvalidations(preload);

      // add 2nd member
      addNewCacheManagerAndWaitForRehash(1, preload);
      printCacheContents();
      printStoreContents();
      checkContentAndInvalidations(preload);

      // add 3rd member
      addNewCacheManagerAndWaitForRehash(2, preload);
      printCacheContents();
      printStoreContents();
      checkContentAndInvalidations(preload);
   }

   private void insertTestData() {
      Cache<String, String> cache = manager(0).getCache(TEST_CACHE_NAME);
      for (int i = 0; i < NUM_KEYS; i++) {
         cache.put("key" + i, Integer.toString(i));
      }

      log.debugf("Added %d entries to test cache", NUM_KEYS);
   }

   private void checkContentAndInvalidations(boolean preload) {
      int clusterSize = getCacheManagers().size();
      int joiner = clusterSize - 1;

      for (int i = 0; i < clusterSize; i++) {
         Cache<String, String> testCache = manager(i).getCache(TEST_CACHE_NAME);
         DistributionManager dm = testCache.getAdvancedCache().getDistributionManager();
         DataContainer dataContainer = testCache.getAdvancedCache().getDataContainer();

         for (int j = 0; j < NUM_KEYS; j++) {
            String key = "key" + j;
            if (!dm.getLocality(key, LookupMode.WRITE).isLocal()) {
               assertFalse("Key '" + key + "' is not owned by node " + address(i) + " but it still appears there",
                     dataContainer.containsKey(key));
            } else if (preload) {
               assertTrue("Key '" + key + "' is owned by node " + address(i) + " but it does not appear there",
                     dataContainer.containsKey(key));
            }
         }
      }

      DummyInMemoryStore store = TestingUtil.getFirstLoader(cache(0, TEST_CACHE_NAME));
      for (int i = 0; i < NUM_KEYS; i++) {
         String key = "key" + i;
         assertTrue("Key " + key + " is missing from the shared store", store.keySet().contains(key));
      }

      log.debugf("Invalidations: %s, L1 invalidations: %s", invalidationCounts, l1InvalidationCounts);
      int joinerSize = advancedCache(joiner, TEST_CACHE_NAME).getDataContainer().size();
      if (preload) {
         // L1 is disabled, so no InvalidateL1Commands
         assertEquals(0, getSum(l1InvalidationCounts));

         // The joiner has preloaded the entire store, and the entries not owned have been invalidated
         assertEquals(NUM_KEYS - joinerSize, getCounter(invalidationCounts, joiner));

         // The other nodes have invalidated the entries moved to the joiner
         if (clusterSize > 1) {
            assertEquals(NUM_KEYS, getSum(invalidationCounts));
         }
      } else {
         // L1 is disabled, so no InvalidateL1Commands
         assertEquals(0, getSum(l1InvalidationCounts));

         // No entries to invalidate on the joiner
         assertEquals(0, getCounter(invalidationCounts, joiner));

         // The other nodes have invalidated the entries moved to the joiner
         if (clusterSize > 1) {
            assertEquals(joinerSize, getSum(invalidationCounts));
         }
      }

      // Reset stats for the next check
      store.clearStats();
      invalidationCounts.clear();
      l1InvalidationCounts.clear();
   }

   private void printCacheContents() {
      log.debugf("%d cache managers: %s", getCacheManagers().size(), getCacheManagers());
      for (int i = 0; i < getCacheManagers().size(); i++) {
         Cache<String, String> testCache = manager(i).getCache(TEST_CACHE_NAME);
         Set<String> keySet = testCache.keySet();
         log.debugf("Cache %s has %d keys: %s", address(i), keySet.size(), keySet);
      }
   }

   private void printStoreContents() {
      DummyInMemoryStore store = TestingUtil.getFirstLoader(cache(0, TEST_CACHE_NAME));
      Set<Object> keySet = store.keySet();
      log.debugf("Shared store has %d keys: %s", keySet.size(), keySet);
   }
}
