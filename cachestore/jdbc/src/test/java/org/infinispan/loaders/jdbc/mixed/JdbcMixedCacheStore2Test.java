package org.infinispan.loaders.jdbc.mixed;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.infinispan.Cache;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStore2Test")
public class JdbcMixedCacheStore2Test extends BaseCacheStoreTest {
   @Override
   protected CacheStore createCacheStore() throws Exception {
      JdbcMixedCacheStoreConfig jdbcCacheStoreConfig = new JdbcMixedCacheStoreConfig();
      TableManipulation stringsTm = UnitTestDatabaseManager.buildStringTableManipulation();
      stringsTm.setTableNamePrefix("STRINGS_TABLE");
      TableManipulation binaryTm = UnitTestDatabaseManager.buildBinaryTableManipulation();
      binaryTm.setTableNamePrefix("BINARY_TABLE");

      ConnectionFactoryConfig cfc = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      jdbcCacheStoreConfig.setConnectionFactoryConfig(cfc);
      jdbcCacheStoreConfig.setStringsTableManipulation(stringsTm);
      jdbcCacheStoreConfig.setBinaryTableManipulation(binaryTm);
      jdbcCacheStoreConfig.setPurgeSynchronously(true);

      JdbcMixedCacheStore cacheStore = new JdbcMixedCacheStore();
      cacheStore.init(jdbcCacheStoreConfig, getCache(), getMarshaller());
      cacheStore.start();
      return cacheStore;
   }
}
