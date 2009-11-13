package org.infinispan.loaders.jdbc.binary;

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
@Test (groups = "functional", testName = "loaders.jdbc.binary.BinaryStoreWithManagedConnectionTest")
public class BinaryStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {
   protected CacheStore createCacheStore() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();
      connectionFactoryConfig.setConnectionFactoryClass(ManagedConnectionFactory.class.getName());
      connectionFactoryConfig.setDatasourceJndiLocation(getDatasourceLocation());
      TableManipulation tm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcBinaryCacheStoreConfig config = new JdbcBinaryCacheStoreConfig(connectionFactoryConfig, tm);
      JdbcBinaryCacheStore jdbcBinaryCacheStore = new JdbcBinaryCacheStore();
      jdbcBinaryCacheStore.init(config, new CacheDelegate("aName"), getMarshaller());
      jdbcBinaryCacheStore.start();
      assert jdbcBinaryCacheStore.getConnectionFactory() instanceof ManagedConnectionFactory;
      return jdbcBinaryCacheStore;
   }


   public void testLoadFromFile() throws Exception {
      CacheManager cm = null;
      try {
         cm = new DefaultCacheManager("configs/managed/binary-managed-connection-factory.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         CacheLoaderConfig firstCacheLoaderConfig = first.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert firstCacheLoaderConfig != null;
         CacheLoaderConfig secondCacheLoaderConfig = second.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcBinaryCacheStoreConfig;
         assert secondCacheLoaderConfig instanceof JdbcBinaryCacheStoreConfig;
         CacheLoaderManager cacheLoaderManager = first.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
         JdbcBinaryCacheStore loader = (JdbcBinaryCacheStore) cacheLoaderManager.getCacheLoader();
         assert loader.getConnectionFactory() instanceof ManagedConnectionFactory;
      } finally {
         try {
            TestingUtil.killCacheManagers(cm);
         } catch (Throwable e) {
            e.printStackTrace();
         }
      }
   }

   @Override
   public String getDatasourceLocation() {
      return "java:/BinaryStoreWithManagedConnectionTest/DS";
   }
}