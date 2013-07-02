package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.stringbased.JdbcStringBasedCacheStoreFunctionalTest")
public class JdbcStringBasedCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tm = UnitTestDatabaseManager.buildStringTableManipulation();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(connectionFactoryConfig, tm);
      return config;
   }
}
