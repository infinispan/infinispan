package org.infinispan.persistence.rocksdb;

import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBStoreFunctionalTest")
public class RocksDBStoreFunctionalTest extends BaseStoreFunctionalTest {

   RocksDBStoreConfigurationBuilder createStoreBuilder(PersistenceConfigurationBuilder loaders) {
      return loaders.addStore(RocksDBStoreConfigurationBuilder.class);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload) {
      createStoreBuilder(persistence)
            .preload(preload);
      return persistence;
   }

   public void testUnknownProperties() {
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      RocksDBStoreConfigurationBuilder storeConfigurationBuilder = createStoreBuilder(cb.persistence());
      storeConfigurationBuilder.addProperty(RocksDBStore.DATABASE_PROPERTY_NAME_WITH_SUFFIX + "unknown", "some_value");
      Configuration c = cb.build();
      String cacheName = "rocksdb-unknown-properties";
      TestingUtil.defineConfiguration(cacheManager, cacheName, c);

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
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testKnownProperties() {
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      RocksDBStoreConfigurationBuilder storeConfigurationBuilder = createStoreBuilder(cb.persistence());
      String dbOptionName = "max_background_compactions";
      String dbOptionValue = "2";
      storeConfigurationBuilder.addProperty(RocksDBStore.DATABASE_PROPERTY_NAME_WITH_SUFFIX + dbOptionName, dbOptionValue);

      String columnFamilyOptionName = "write_buffer_size";
      String columnFamilyOptionValue = "96MB";
      storeConfigurationBuilder.addProperty(RocksDBStore.COLUMN_FAMILY_PROPERTY_NAME_WITH_SUFFIX + columnFamilyOptionName, columnFamilyOptionValue);

      Configuration c = cb.build();
      String cacheName = "rocksdb-known-properties";
      TestingUtil.defineConfiguration(cacheManager, cacheName, c);

      // No easy way to ascertain if options are set, however if cache starts up it must have applied them,
      // since otherwise this will fail like the unkonwn properties method
      assertNotNull(cacheManager.getCache(cacheName));
   }
}
