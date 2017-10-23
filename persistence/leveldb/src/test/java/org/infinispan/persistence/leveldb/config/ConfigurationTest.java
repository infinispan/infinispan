package org.infinispan.persistence.leveldb.config;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.infinispan.Cache;
import org.infinispan.commons.test.skip.SkipOnOs;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
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
@SkipOnOs({SkipOnOs.OS.SOLARIS, SkipOnOs.OS.WINDOWS})
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
      InputStream configSTream = ConfigurationTest.class.getResourceAsStream("/config/leveldb-config-auto.xml");
      ConfigurationBuilderHolder configHolder = new ParserRegistry().parse(configSTream);

      // check persistence attributes
      Configuration cacheConfig = configHolder.getNamedConfigurationBuilders().get("testCache").build();
      assertFalse(cacheConfig.persistence().passivation());
      assertEquals(cacheConfig.persistence().stores().size(), 1);

      // check generic store attributes
      StoreConfiguration cacheLoaderConfig = cacheConfig.persistence().stores().get(0);
      assertFalse(cacheLoaderConfig.shared());
      assertTrue(cacheLoaderConfig.preload());
      assertTrue(cacheLoaderConfig instanceof LevelDBStoreConfiguration);

      // check LevelDB store attributes
      LevelDBStoreConfiguration leveldbConfig = (LevelDBStoreConfiguration) cacheLoaderConfig;
      assertEquals("/tmp/leveldb/52/data", leveldbConfig.location());
      assertEquals("/tmp/leveldb/52/expired", leveldbConfig.expiredLocation());
      assertEquals(LevelDBStoreConfiguration.ImplementationType.AUTO, leveldbConfig.implementationType());
   }
}
