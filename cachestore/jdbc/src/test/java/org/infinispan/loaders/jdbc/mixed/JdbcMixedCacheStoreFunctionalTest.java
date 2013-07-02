package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreFunctionalTest")
public class JdbcMixedCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      TableManipulation stringsTm = UnitTestDatabaseManager.buildStringTableManipulation();
      stringsTm.setTableNamePrefix("STRINGS_TABLE");
      TableManipulation binaryTm = UnitTestDatabaseManager.buildBinaryTableManipulation();
      binaryTm.setTableNamePrefix("BINARY_TABLE");
      ConnectionFactoryConfig cfc = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      JdbcMixedCacheStoreConfig jdbcCacheStoreConfig = new JdbcMixedCacheStoreConfig(cfc, binaryTm, stringsTm);
      jdbcCacheStoreConfig.setConnectionFactoryConfig(cfc);
      jdbcCacheStoreConfig.setStringsTableManipulation(stringsTm);
      jdbcCacheStoreConfig.setBinaryTableManipulation(binaryTm);
      return jdbcCacheStoreConfig;
   }

}
