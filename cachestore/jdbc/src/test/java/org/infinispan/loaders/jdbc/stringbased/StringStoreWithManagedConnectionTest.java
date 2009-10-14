package org.infinispan.loaders.jdbc.stringbased;

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
@Test (groups = "functional", testName = "loaders.jdbc.stringbased.StringStoreWithManagedConnectionTest")
public class StringStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   protected CacheStore createCacheStore() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();
      connectionFactoryConfig.setConnectionFactoryClass(ManagedConnectionFactory.class.getName());
      connectionFactoryConfig.setDatasourceJndiLocation(DATASOURCE_LOCATION);
      TableManipulation tm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(connectionFactoryConfig, tm);
      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      stringBasedCacheStore.init(config, new CacheDelegate("aName"), getMarshaller());
      stringBasedCacheStore.start();
      return stringBasedCacheStore;
   }

   public void testLoadFromFile() throws Exception {
      CacheManager cm = null;
      try {
         cm = new DefaultCacheManager("configs/str-managed-connection-factory.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         CacheLoaderConfig firstCacheLoaderConfig = first.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert firstCacheLoaderConfig != null;
         CacheLoaderConfig secondCacheLoaderConfig = second.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcStringBasedCacheStoreConfig;
         assert secondCacheLoaderConfig instanceof JdbcStringBasedCacheStoreConfig;
         CacheLoaderManager cacheLoaderManager = first.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
         JdbcStringBasedCacheStore loader = (JdbcStringBasedCacheStore) cacheLoaderManager.getCacheLoader();
         assert loader.getConnectionFactory() instanceof ManagedConnectionFactory;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
