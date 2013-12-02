package org.infinispan.persistence.leveldb.config;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test(groups = "unit", testName = "persistence.leveldb.configuration.ConfigurationTest")
public class ConfigurationTest extends AbstractInfinispanTest {
   private String tmpDirectory;
   private String tmpDataDirectory;
   private String tmpExpiredDirectory;

   @BeforeTest
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this.getClass());
      tmpDataDirectory = tmpDirectory + "/data";
      tmpExpiredDirectory = tmpDirectory + "/expired";
   }

   @AfterTest(alwaysRun = true)
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
   }

   public void testConfigBuilder() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder().globalJmxStatistics().transport().defaultTransport().build();

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

   @Test(enabled = false, description = "ISPN-3388")
   public void testLegacyJavaConfig() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder().globalJmxStatistics().transport().defaultTransport().build();

      Configuration cacheConfig = new ConfigurationBuilder().persistence().addStore(LevelDBStoreConfigurationBuilder.class).addProperty("location", tmpDataDirectory)
            .addProperty("expiredLocation", tmpExpiredDirectory).addProperty("implementationType", LevelDBStoreConfiguration.ImplementationType.AUTO.toString()).build();

      EmbeddedCacheManager cacheManager = new DefaultCacheManager(globalConfig);

      cacheManager.defineConfiguration("testCache", cacheConfig);

      cacheManager.start();
      Cache<String, String> cache = cacheManager.getCache("testCache");

      cache.put("hello", "there legacy java");
      cache.stop();
      cacheManager.stop();
   }

   @Test(enabled = false, description = "ISPN-3388")
   public void textXmlConfigLegacy() throws IOException {
      EmbeddedCacheManager cacheManager = new DefaultCacheManager("config/leveldb-config-legacy-" +
            LevelDBStoreConfiguration.ImplementationType.AUTO.toString().toLowerCase() + ".xml");
      Cache<String, String> cache = cacheManager.getCache("testCache");

      cache.put("hello", "there legacy xml");
      cache.stop();
      cacheManager.stop();

      TestingUtil.recursiveFileRemove("/tmp/leveldb/legacy");
   }

   public void testXmlConfig60() throws IOException {
      EmbeddedCacheManager cacheManager = new DefaultCacheManager("config/leveldb-config-60-" +
            LevelDBStoreConfiguration.ImplementationType.AUTO.toString().toLowerCase() + ".xml");

      Cache<String, String> cache = cacheManager.getCache("testCache");

      cache.put("hello", "there 60 xml");
      cache.stop();
      cacheManager.stop();

      TestingUtil.recursiveFileRemove("/tmp/leveldb/60");
   }

}
