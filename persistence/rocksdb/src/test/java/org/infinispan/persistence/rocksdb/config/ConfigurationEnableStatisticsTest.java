package org.infinispan.persistence.rocksdb.config;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.rocksdb.RocksDBStore;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.rocksdb.TickerType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "unit", testName = "persistence.rocksdb.config.ConfigurationEnableStatisticsTest")
public class ConfigurationEnableStatisticsTest extends AbstractInfinispanTest {

   private String tmpDirectory = TestingUtil.tmpDirectory(this.getClass());

   public void testRocksDbEnableStatistics() {

      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(tmpDirectory);
      global.defaultCacheName("defaultCache");

      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cb.persistence()
         .addStore(RocksDBStoreConfigurationBuilder.class)
         .enableStatistics(true)
         .location(tmpDirectory)
         .expiredLocation(tmpDirectory);

      EmbeddedCacheManager cacheManager = new DefaultCacheManager(global.build(), new ConfigurationBuilder().build());
      cacheManager.defineConfiguration("weather", cb.build());
      Cache<String, String> cache = cacheManager.getCache("weather");
      cache.put("foo", "bar");

      RocksDBStore store = TestingUtil.getFirstLoader(cache);
      assertTrue(store.getDbStatistics().getTickerCount(TickerType.BYTES_WRITTEN) > 0);
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }
}
