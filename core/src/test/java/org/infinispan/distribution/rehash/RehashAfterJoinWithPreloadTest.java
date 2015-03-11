package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LookupMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Test with a distributed cache (numOwners=1), a shared cache store and 'preload' enabled
 * (ISPN-1964).
 *
 * @author Carsten Lohmann
 */
@Test(testName = "distribution.rehash.RehashAfterJoinWithPreloadTest", groups = "functional")
public class RehashAfterJoinWithPreloadTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(RehashAfterJoinWithPreloadTest.class);

   public static final int NUM_KEYS = 20;
   private final String testCacheName = "testCache" + getClass().getSimpleName();

   private final String fileCacheStoreTmpDir = TestingUtil.tmpDirectory(this.getClass());

   @Override
   protected void createCacheManagers() {
      // cacheManagers started one after another in test()
   }

   private void addNewCacheManagerAndWaitForRehash() {
      EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(getDefaultClusteredCacheConfig(
            CacheMode.DIST_SYNC, false));
      cacheManager.defineConfiguration(testCacheName, buildCfg(true));
      log.debugf("\n\nstarted CacheManager #%d", getCacheManagers().size() - 1);
      waitForClusterToForm(testCacheName);
   }

   private Configuration buildCfg(boolean clustered) {
      ConfigurationBuilder cb = new ConfigurationBuilder();

      SingleFileStoreConfigurationBuilder fileStoreCB = cb.persistence()
            .addSingleFileStore()
              .location(fileCacheStoreTmpDir)
              .preload(true).shared(true);
      fileStoreCB.purgeOnStartup(false);

      cb.persistence().passivation(false);

      if (clustered) {
         cb.clustering().l1().disable();
         cb.clustering().cacheMode(CacheMode.DIST_SYNC);
         cb.clustering().hash().numOwners(1); // one owner!

         cb.clustering().stateTransfer().fetchInMemoryState(true);
         cb.clustering().hash().groups().enabled();
      }
      return cb.build(true);
   }

   public void test() {
      // start a cluster that uses this cache store
      // add 1st member
      addNewCacheManagerAndWaitForRehash();

      // insert the data in the cache and check the contents
      putTestDataInCacheStore();
      printCacheContents();

      // stop the 1st member
      killMember(0);

      // re-add the 1st member
      addNewCacheManagerAndWaitForRehash();
      printCacheContents();
      assertEvenDistribution();

      // add 2nd member
      addNewCacheManagerAndWaitForRehash();
      printCacheContents();
      assertEvenDistribution();

      // add 3rd member
      addNewCacheManagerAndWaitForRehash();
      printCacheContents();
      assertEvenDistribution();
   }

   private void assertEvenDistribution() {
      for (int i = 0; i < getCacheManagers().size(); i++) {
         Cache<String, String> testCache = manager(i).getCache(testCacheName);
         DistributionManager dm = testCache.getAdvancedCache().getDistributionManager();
         DataContainer dataContainer = testCache.getAdvancedCache().getDataContainer();

         // Note there is stale data in the cache store that this owner no longer owns
         for (int j = 0; j < NUM_KEYS; j++) {
            String key = "key" + j;
            // each key must only occur once (numOwners is one)
            if (dm.getLocality(key, LookupMode.WRITE).isLocal()) {
               assertTrue("Key '" + key + "' is owned by node " + address(i) + " but it doesn't appears there",
                     dataContainer.containsKey(key));
            } else {
               assertTrue("Key '" + key + "' is not owned by node " + address(i) + " but it still appears there",
                     !dataContainer.containsKey(key));
            }
         }
      }
   }

   private void putTestDataInCacheStore() {
      Cache<String, String> cache = manager(0).getCache(testCacheName);
      for (int i = 0; i < NUM_KEYS; i++) {
         cache.put("key" + i, Integer.toString(i));
      }

      log.debugf("added %d entries to test cache", NUM_KEYS);
   }

   private void printCacheContents() {
      log.debugf("%d cache manager(s)", getCacheManagers().size());
      for (int i = 0; i < getCacheManagers().size(); i++) {
         Cache<String, String> testCache = manager(i).getCache(testCacheName);
         log.debugf(" Contents of Cache with CacheManager #%d (%s, all members: %s)", i, address(i),
               testCache.getAdvancedCache().getRpcManager().getMembers());
         Set<String> keySet = testCache.keySet();
         log.debugf(" keySet = %s", keySet);
         for (String key : keySet) {
            log.debugf("  key: %s  value: %s", key, testCache.get(key));
         }
      }
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(fileCacheStoreTmpDir);
   }
}
