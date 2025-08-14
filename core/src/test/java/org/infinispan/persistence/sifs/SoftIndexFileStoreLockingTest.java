package org.infinispan.persistence.sifs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreLockingTest")
public class SoftIndexFileStoreLockingTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "locking-test-cache";
   protected String tmpDirectory;

   @AfterClass(alwaysRun = true, dependsOnMethods = "destroyAfterClass")
   protected void clearTempDir() throws IOException {
      try {
         SoftIndexFileStoreTestUtils.StatsValue statsValue = SoftIndexFileStoreTestUtils.readStatsFile(tmpDirectory, CACHE_NAME, log);
         long dataDirectorySize = SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, CACHE_NAME);

         assertEquals(dataDirectorySize, statsValue.getStatsSize());
      } finally {
         Util.recursiveFileRemove(tmpDirectory);
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().enable().persistentLocation(CommonsTestingUtil.tmpDirectory(this.getClass()));
      EmbeddedCacheManager ecm = TestCacheManagerFactory.newDefaultCacheManager(true, global, new ConfigurationBuilder());
      TestingUtil.defineConfiguration(ecm, CACHE_NAME, createCacheConfiguration().build());
      return ecm;
   }

   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, String cacheName, boolean preload) {
      persistence
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(1000)
            .async().enabled(true)
            .purgeOnStartup(false).preload(preload)
            // Effectively disable reaper for tests
            .expiration().wakeUpInterval(Long.MAX_VALUE);
      return persistence;
   }

   private ConfigurationBuilder createCacheConfiguration() {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), CACHE_NAME, false);
      return cb;
   }

   public void testOverlappingCacheManagers() throws Throwable {
      // Get the cache with the default cache manager created by the test.
      Cache<String, Object> c1 = cacheManager.getCache(CACHE_NAME);
      c1.put("key", "value");

      // Create another cache manager utilizing the same configuration.
      // This will cause both CM to utilize the same directory to store data.
      EmbeddedCacheManager ecm = createCacheManager();

      // It is not possible to retrieve the running cache.
      Exceptions.expectException("ISPN029025: Failed acquiring lock .*", () -> ecm.getCache(CACHE_NAME),
            PersistenceException.class);
      TestingUtil.killCacheManagers(ecm);

      // The original cache still works properly.
      assertThat(c1.get("key")).isEqualTo("value");
   }

   public void testStartStopDifferentCacheManagers() throws Throwable {
      // Acquire the cache with the cache manager created by the test.
      Cache<String, Object> c1 = cacheManager.getCache(CACHE_NAME);
      c1.put("key", "value");

      // Stop the manager and release the file lock.
      TestingUtil.killCacheManagers(cacheManager);

      // Create another CM.
      // This time it is possible to retrieve the running cache instance.
      EmbeddedCacheManager ecm = createCacheManager();
      c1 = ecm.getCache(CACHE_NAME);

      // The data still present.
      assertThat(c1.get("key")).isEqualTo("value");
      TestingUtil.killCacheManagers(ecm);
   }
}
