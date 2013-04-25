/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
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

   protected boolean supportsConcurrentUpdates = true;

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

      FileCacheStoreConfigurationBuilder fileStoreCB = cb.loaders().addFileCacheStore().location(fileCacheStoreTmpDir);
      fileStoreCB.purgeOnStartup(false);

      cb.loaders().passivation(false);
      cb.loaders().preload(true);
      cb.loaders().shared(true);

      if (clustered) {
         cb.clustering().l1().disable();
         cb.clustering().cacheMode(CacheMode.DIST_SYNC);
         cb.clustering().hash().numOwners(1); // one owner!

         cb.clustering().stateTransfer().fetchInMemoryState(true);
         cb.clustering().hash().groups().enabled();
      }
      cb.locking().supportsConcurrentUpdates(supportsConcurrentUpdates);
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
         for (String key : testCache.keySet()) {
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
            .getDefaultConfiguration(true));
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

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(fileCacheStoreTmpDir);
   }
}