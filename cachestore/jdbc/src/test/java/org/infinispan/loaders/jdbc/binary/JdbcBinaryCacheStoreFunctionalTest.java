package org.infinispan.loaders.jdbc.binary;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * JdbcBinaryCacheStoreFunctionalTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "loaders.jdbc.binary.JdbcBinaryCacheStoreFunctionalTest")
public class JdbcBinaryCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tm = UnitTestDatabaseManager.buildBinaryTableManipulation();
      JdbcBinaryCacheStoreConfig config = new JdbcBinaryCacheStoreConfig(connectionFactoryConfig, tm);
      return config;
//      JdbcBinaryCacheStore jdbcBucketCacheStore = new JdbcBinaryCacheStore();
//      jdbcBucketCacheStore.init(config, new CacheDelegate("aName"), getMarshaller());
//      jdbcBucketCacheStore.start();
//      assert jdbcBucketCacheStore.getConnectionFactory() != null;
//      return jdbcBucketCacheStore;
   }
}
