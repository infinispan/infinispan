package org.infinispan.persistence.rocksdb;

import static org.testng.AssertJUnit.assertNotNull;

import java.io.File;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.rocksdb.RocksDBException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBStoreFunctionalTest")
public class RocksDBStoreFunctionalTest extends BaseStoreFunctionalTest {
   protected String tmpDirectory = TestingUtil.tmpDirectory(this.getClass());

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   RocksDBStoreConfigurationBuilder createStoreBuilder(PersistenceConfigurationBuilder loaders) {
      new File(tmpDirectory).mkdirs();
      return loaders.addStore(RocksDBStoreConfigurationBuilder.class).location(tmpDirectory + "/data").expiredLocation(tmpDirectory + "/expiry").clearThreshold(2);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      createStoreBuilder(persistence)
            .preload(preload);
      return persistence;
   }

   public void testUnknownProperties() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.read(cacheManager.getDefaultCacheConfiguration());
      new File(tmpDirectory).mkdirs();
      RocksDBStoreConfigurationBuilder storeConfigurationBuilder = createStoreBuilder(cb.persistence());
      storeConfigurationBuilder.addProperty(RocksDBStore.databasePropertyNameWithSuffix + "unknown", "some_value");
      Configuration c = cb.build();
      String cacheName = "rocksdb-unknown-properties";
      cacheManager.defineConfiguration(cacheName, c);

      try {
         cacheManager.getCache(cacheName);
      } catch (Throwable t) {
         Throwable cause;
         while ((cause = t.getCause()) != null) {
            t = cause;
         }
         Exceptions.assertException(CacheConfigurationException.class, ".*unknown\\ property$", t);
      }

      // Stop the cache manager early, otherwise cleanup won't work properly
      cacheManager.stop();
   }

   public void testKnownProperties() throws RocksDBException {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.read(cacheManager.getDefaultCacheConfiguration());
      new File(tmpDirectory).mkdirs();
      RocksDBStoreConfigurationBuilder storeConfigurationBuilder = createStoreBuilder(cb.persistence());
      String dbOptionName = "max_background_compactions";
      String dbOptionValue = "2";
      storeConfigurationBuilder.addProperty(RocksDBStore.databasePropertyNameWithSuffix + dbOptionName, dbOptionValue);

      String columnFamilyOptionName = "write_buffer_size";
      String columnFamilyOptionValue = "96MB";
      storeConfigurationBuilder.addProperty(RocksDBStore.columnFamilyPropertyNameWithSuffix + columnFamilyOptionName, columnFamilyOptionValue);

      Configuration c = cb.build();
      String cacheName = "rocksdb-known-properties";
      cacheManager.defineConfiguration(cacheName, c);

      // No easy way to ascertain if options are set, however if cache starts up it must have applied them,
      // since otherwise this will fail like the unkonwn properties method
      assertNotNull(cacheManager.getCache(cacheName));
   }
}
