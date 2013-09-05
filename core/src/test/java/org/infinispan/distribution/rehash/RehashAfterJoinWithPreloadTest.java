package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SingleFileStoreConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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

   private final String testCacheName = "testCache" + getClass().getSimpleName();

   private final String fileCacheStoreTmpDir = TestingUtil.tmpDirectory(this);

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
      // first initialize the file based cache store with 3 entries in a cache
      putTestDataInCacheStore();

      // start a cluster that uses this cache store
      // add 1st member
      addNewCacheManagerAndWaitForRehash();
      printCacheContents();

      // add 2nd member
      addNewCacheManagerAndWaitForRehash();
      printCacheContents();

      // add 3rd member
      addNewCacheManagerAndWaitForRehash();
      printCacheContents();
      assertEvenDistribution();
   }

   private void assertEvenDistribution() {
      for (int i = 0; i < getCacheManagers().size(); i++) {
         Cache<String, String> testCache = manager(i).getCache(testCacheName);
         DistributionManager dm = testCache.getAdvancedCache().getDistributionManager();
         // Note there is stale data in the cache store that this owner no longer owns
         for (Object key : testCache.getAdvancedCache().getDataContainer().keySet()) {
            // each key must only occur once (numOwners is one)
            assertTrue("Key '" + key + "' is not owned by node " + address(i) + " but it still appears there",
                  dm.getLocality(key).isLocal());
         }
      }
   }

   private void putTestDataInCacheStore() {
      final int numKeys = 20;
      log.debugf("Using cache store dir %s", fileCacheStoreTmpDir);
      EmbeddedCacheManager cmForCacheStoreInit = TestCacheManagerFactory.createCacheManager(TestCacheManagerFactory
            .getDefaultCacheConfiguration(true));
      try {
         cmForCacheStoreInit.defineConfiguration(testCacheName, buildCfg(false));

         Cache<String, String> cache = cmForCacheStoreInit.getCache(testCacheName);
         for (int i = 0; i < numKeys; i++) {
            cache.put("key" + i, Integer.toString(i));
         }

         log.debugf("added %d entries to test cache", numKeys);
      } finally {
         TestingUtil.killCacheManagers(cmForCacheStoreInit);
      }
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