package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreTest2")
public class JdbcMixedCacheStoreTest2 extends BaseCacheStoreTest {
   protected CacheStore createCacheStore() throws Exception {
      JdbcMixedCacheStoreConfig jdbcCacheStoreConfig = new JdbcMixedCacheStoreConfig();
      TableManipulation stringsTm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      stringsTm.setTableNamePrefix("STRINGS_TABLE");
      TableManipulation binaryTm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      binaryTm.setTableNamePrefix("BINARY_TABLE");

      ConnectionFactoryConfig cfc = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      jdbcCacheStoreConfig.setConnectionFactoryConfig(cfc);
      jdbcCacheStoreConfig.setStringsTableManipulation(stringsTm);
      jdbcCacheStoreConfig.setBinaryTableManipulation(binaryTm);

      JdbcMixedCacheStore cacheStore = new JdbcMixedCacheStore();
      cacheStore.init(jdbcCacheStoreConfig, null, getMarshaller());
      cacheStore.start();
      return cacheStore;
   }
}
