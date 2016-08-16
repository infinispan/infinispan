package org.infinispan.persistence.leveldb.config;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test(groups = "unit", testName = "persistence.leveldb.configuration.ConfigurationTest")
public class ConfigurationTest extends AbstractInfinispanTest {
   private String tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
   private String tmpDataDirectory = tmpDirectory + "/data";
   private String tmpExpiredDirectory = tmpDirectory + "/expired";

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   public void testConfigBuilder() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .globalJmxStatistics().allowDuplicateDomains(true)
            .transport().defaultTransport()
            .build();

      Configuration cacheConfig = new ConfigurationBuilder().persistence().addStore(LevelDBStoreConfigurationBuilder.class).location(tmpDataDirectory)
            .expiredLocation(tmpExpiredDirectory).implementationType(LevelDBStoreConfiguration.ImplementationType.AUTO).build();

      StoreConfiguration cacheLoaderConfig = cacheConfig.persistence().stores().get(0);
      assertTrue(cacheLoaderConfig instanceof LevelDBStoreConfiguration);
      LevelDBStoreConfiguration leveldbConfig = (LevelDBStoreConfiguration) cacheLoaderConfig;
      assertEquals(tmpDataDirectory, leveldbConfig.location());
      assertEquals(tmpExpiredDirectory, leveldbConfig.expiredLocation());

      EmbeddedCacheManager cacheManager = new DefaultCacheManager(globalConfig);

      cacheManager.defineConfiguration("testCache", cacheConfig);

      cacheManager.start();
      Cache<String, String> cache = cacheManager.getCache("testCache");

      cache.put("hello", "there");
      cache.stop();
      cacheManager.stop();
   }

   public void testXmlConfig() throws IOException {
      try {
         EmbeddedCacheManager cacheManager = new DefaultCacheManager("config/leveldb-config-" +
                 LevelDBStoreConfiguration.ImplementationType.AUTO.toString().toLowerCase() + ".xml");

         Cache<String, String> cache = cacheManager.getCache("testCache");

         cache.put("hello", "there 52 xml");
         cache.stop();
         cacheManager.stop();
      } finally {
         Util.recursiveFileRemove("/tmp/leveldb/52");
      }
   }

}
