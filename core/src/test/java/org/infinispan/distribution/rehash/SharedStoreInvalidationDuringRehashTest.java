package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Test that entries in a shared store are not touched in any way during state transfer.
 *
 * @author Dan Berindei
 */
@Test(testName = "distribution.rehash.SharedStoreInvalidationDuringRehashTest", groups = "functional")
@CleanupAfterMethod
@InCacheMode({CacheMode.DIST_SYNC })
public class SharedStoreInvalidationDuringRehashTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(SharedStoreInvalidationDuringRehashTest.class);

   private static final int NUM_KEYS = 20;
   private static final String TEST_CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() {
      // cacheManagers started one after another in test()
   }

   private void addNewCacheManagerAndWaitForRehash(int index, boolean preload) {
      EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(getDefaultClusteredCacheConfig(
            cacheMode, false));
      Configuration config = buildCfg(index, true, preload);
      cacheManager.defineConfiguration(TEST_CACHE_NAME, config);
      log.debugf("\n\nstarted CacheManager #%d", getCacheManagers().size() - 1);
      waitForClusterToForm(TEST_CACHE_NAME);
   }

   private Configuration buildCfg(final int index, boolean clustered, boolean preload) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.persistence().passivation(false);

      DummyInMemoryStoreConfigurationBuilder dummyCB = cb.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      dummyCB.preload(preload).shared(true).purgeOnStartup(false);
      dummyCB.storeName(SharedStoreInvalidationDuringRehashTest.class.getSimpleName());

      if (clustered) {
         cb.clustering().l1().disable();
         cb.clustering().cacheMode(cacheMode);
         cb.clustering().hash().numOwners(1); // one owner!

         cb.clustering().stateTransfer().fetchInMemoryState(true);
         cb.clustering().hash().groups().enabled();
      }
      return cb.build(true);
   }

   private void incrementCounter(ConcurrentMap<Integer, ConcurrentMap<Object, AtomicInteger>> counterMap, int index, Object[] keys) {
      ConcurrentMap<Object, AtomicInteger> counters = counterMap.computeIfAbsent(index, ignored -> new ConcurrentHashMap<>());
      for (Object key : keys) {
         counters.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
      }
   }

   private int getCounter(ConcurrentMap<Integer, ConcurrentMap<Object, AtomicInteger>> counterMap, int index) {
      ConcurrentMap<Object, AtomicInteger> counters = counterMap.get(index);
      return counters == null ? 0 : counters.values().stream().mapToInt(AtomicInteger::get).sum();
   }

   private int getSum(ConcurrentMap<Integer, ConcurrentMap<Object, AtomicInteger>> counterMap) {
      return counterMap.values().stream().flatMapToInt(
            m -> m.values().stream().mapToInt(AtomicInteger::get)
      ).sum();
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

      HashMap<Object, Integer> currentOwners = new HashMap<>();
      for (int i = 0; i < clusterSize; i++) {
         Cache<String, String> testCache = manager(i).getCache(TEST_CACHE_NAME);
         DistributionManager dm = testCache.getAdvancedCache().getDistributionManager();
         DataContainer dataContainer = testCache.getAdvancedCache().getDataContainer();

         for (int j = 0; j < NUM_KEYS; j++) {
            String key = "key" + j;
            if (!dm.getCacheTopology().isReadOwner(key)) {
               assertFalse("Key '" + key + "' is not owned by node " + address(i) + " but it still appears there",
                     dataContainer.containsKey(key));
            } else {
               currentOwners.put(key, i);
               if (preload) {
                  assertTrue("Key '" + key + "' is owned by node " + address(i) + " but it does not appear there",
                        dataContainer.containsKey(key));
               }
            }
         }
      }

      DummyInMemoryStore store = TestingUtil.getFirstStore(cache(0, TEST_CACHE_NAME));
      for (int i = 0; i < NUM_KEYS; i++) {
         String key = "key" + i;
         assertTrue("Key " + key + " is missing from the shared store", store.keySet().contains(key));
      }

      // Reset stats for the next check
      store.clearStats();
   }

   private int computeDiff(Map<Object, Integer> previous, Map<Object, Integer> current) {
      assertEquals(previous.size(), current.size());
      int diff = 0;
      for (Map.Entry<Object, Integer> pair : previous.entrySet()) {
         if (Integer.compare(pair.getValue(), current.get(pair.getKey())) != 0) ++diff;
      }
      return diff;
   }

   private void printCacheContents() {
      log.debugf("%d cache managers: %s", getCacheManagers().size(), getCacheManagers());
      for (int i = 0; i < getCacheManagers().size(); i++) {
         Cache<String, String> testCache = manager(i).getCache(TEST_CACHE_NAME);
         DataContainer<String, String> dataContainer = testCache.getAdvancedCache().getDataContainer();
         log.debugf("DC on %s has %d keys: %s", address(i), dataContainer.size(),
               StreamSupport.stream(dataContainer.spliterator(), false).map(Map.Entry::getKey).collect(Collectors.joining(",")));
         Set<String> keySet = testCache.keySet();
         log.debugf("Cache %s has %d keys: %s", address(i), keySet.size(), keySet);
      }
   }

   private void printStoreContents() {
      DummyInMemoryStore store = TestingUtil.getFirstStore(cache(0, TEST_CACHE_NAME));
      Set<Object> keySet = store.keySet();
      log.debugf("Shared store has %d keys: %s", keySet.size(), keySet);
   }
}
