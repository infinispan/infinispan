package org.horizon.loader.jdbc.mixed;

import org.horizon.loader.BaseCacheStoreTest;
import org.horizon.loader.CacheStore;
import org.horizon.loader.jdbc.TableManipulation;
import org.horizon.loader.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.horizon.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loader.jdbc.mixed.JdbcMixedCacheStoreTest2")
public class JdbcMixedCacheStoreTest2 extends BaseCacheStoreTest {
   protected CacheStore createCacheStore() throws Exception {
      JdbcMixedCacheStoreConfig jdbcCacheStoreConfig = new JdbcMixedCacheStoreConfig();
      TableManipulation stringsTm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      stringsTm.setTableName("STRINGS_TABLE");
      TableManipulation binaryTm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      binaryTm.setTableName("BINARY_TABLE");

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
