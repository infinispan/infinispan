package org.infinispan.persistence.rocksdb.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;

import org.infinispan.Cache;
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
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfiguration;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.configuration.ConfigurationTest")
public class ConfigurationTest extends AbstractInfinispanTest {
   private final String tmpDirectory = Testing.tmpDirectory(this.getClass());
   private final String tmpDataDirectory = tmpDirectory + "/data";
   private final String tmpExpiredDirectory = tmpDirectory + "/expired";

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   public void testConfigBuilder() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .transport().defaultTransport()
            .globalState().persistentLocation(tmpDirectory)
            .build();

      Configuration cacheConfig = new ConfigurationBuilder().persistence().addStore(RocksDBStoreConfigurationBuilder.class).location(tmpDataDirectory)
            .expiredLocation(tmpExpiredDirectory).build();

      StoreConfiguration cacheLoaderConfig = cacheConfig.persistence().stores().get(0);
      assertInstanceOf(RocksDBStoreConfiguration.class, cacheLoaderConfig);
      RocksDBStoreConfiguration rocksdbConfig = (RocksDBStoreConfiguration) cacheLoaderConfig;
      assertEquals(tmpDataDirectory, rocksdbConfig.location());
      assertEquals(tmpExpiredDirectory, rocksdbConfig.expiredLocation());

      EmbeddedCacheManager cacheManager = new DefaultCacheManager(globalConfig);

      cacheManager.defineConfiguration("testCache", cacheConfig);

      cacheManager.start();
      Cache<String, String> cache = cacheManager.getCache("testCache");

      cache.put("hello", "there");
      cache.stop();
      cacheManager.stop();
   }

   public void testXmlConfig() throws IOException {
      URL config = ConfigurationTest.class.getResource("/configs/all/rocksdb-config.xml");
      ConfigurationBuilderHolder configHolder = new ParserRegistry().parse(config);

      // check persistence attributes
      Configuration cacheConfig = configHolder.getNamedConfigurationBuilders().get("testCache").build();
      assertFalse(cacheConfig.persistence().passivation());
      assertEquals(1, cacheConfig.persistence().stores().size());

      // check generic store attributes
      StoreConfiguration cacheLoaderConfig = cacheConfig.persistence().stores().get(0);
      assertFalse(cacheLoaderConfig.shared());
      assertTrue(cacheLoaderConfig.preload());
      assertInstanceOf(RocksDBStoreConfiguration.class, cacheLoaderConfig);

      // check RocksDB store attributes
      RocksDBStoreConfiguration rocksdbConfig = (RocksDBStoreConfiguration) cacheLoaderConfig;
      assertEquals("/tmp/rocksdb/52/data", rocksdbConfig.location());
      assertEquals("/tmp/rocksdb/52/expired", rocksdbConfig.expiredLocation());
   }
}
