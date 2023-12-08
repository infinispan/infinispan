package org.infinispan.persistence.sifs;

import java.nio.file.Paths;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.sifs.SoftIndexFileStoreFunctionalTest")
public class SoftIndexFileStoreFunctionalTest extends BaseStoreFunctionalTest {
   protected String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload) {
      persistence
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .purgeOnStartup(false).preload(preload)
            // Effectively disable reaper for tests
            .expiration().wakeUpInterval(Long.MAX_VALUE);
      return persistence;
   }

   public void testWritingSameKeyShortTimes() {
      String cacheName = "testWritingSameKeyShortTimes";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);

      for (int i = 0; i < Short.MAX_VALUE + 1; ++i) {
         cache.put("k", "v");
      }

      cache.remove("k");
   }
}
