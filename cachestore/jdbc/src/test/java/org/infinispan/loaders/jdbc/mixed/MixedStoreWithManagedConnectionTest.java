package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.CacheDelegate;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.mixed.MixedStoreWithManagedConnectionTest")
public class MixedStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   protected CacheStore createCacheStore() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();
      connectionFactoryConfig.setConnectionFactoryClass(ManagedConnectionFactory.class.getName());
      connectionFactoryConfig.setDatasourceJndiLocation(DATASOURCE_LOCATION);
      TableManipulation stringsTm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      stringsTm.setTableNamePrefix("STRINGS_TABLE");
      TableManipulation binaryTm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      binaryTm.setTableNamePrefix("BINARY_TABLE");
      JdbcMixedCacheStoreConfig cacheStoreConfig = new JdbcMixedCacheStoreConfig(connectionFactoryConfig, binaryTm, stringsTm);
      JdbcMixedCacheStore store = new JdbcMixedCacheStore();
      store.init(cacheStoreConfig, new CacheDelegate("aName"), getMarshaller());
      store.start();
      assert store.getConnectionFactory() instanceof ManagedConnectionFactory;
      return store;
   }


   public void testLoadFromFile() throws Exception {
      CacheManager cm = null;
      try {
         cm = new DefaultCacheManager("configs/mixed-managed-connection-factory.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         CacheLoaderConfig firstCacheLoaderConfig = first.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert firstCacheLoaderConfig != null;
         CacheLoaderConfig secondCacheLoaderConfig = second.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcMixedCacheStoreConfig;
         assert secondCacheLoaderConfig instanceof JdbcMixedCacheStoreConfig;
         CacheLoaderManager cacheLoaderManager = first.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
         JdbcMixedCacheStore loader = (JdbcMixedCacheStore) cacheLoaderManager.getCacheLoader();
         assert loader.getConnectionFactory() instanceof ManagedConnectionFactory;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
