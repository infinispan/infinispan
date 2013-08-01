package org.infinispan.loaders.jdbc.binary;

import org.infinispan.Cache;
import org.infinispan.CacheImpl;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.loaders.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.configuration.ManagedConnectionFactoryConfigurationBuilder;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.binary.BinaryStoreWithManagedConnectionTest")
public class BinaryStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {
   @Override
   protected CacheStore createCacheStore() throws Exception {
      JdbcBinaryCacheStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
               .addLoader(JdbcBinaryCacheStoreConfigurationBuilder.class)
               .purgeSynchronously(true);

      storeBuilder.dataSource().jndiUrl(getDatasourceLocation());
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), true);

      JdbcBinaryCacheStore jdbcBinaryCacheStore = new JdbcBinaryCacheStore();
      jdbcBinaryCacheStore.init(storeBuilder.create(), getCache(), getMarshaller());
      jdbcBinaryCacheStore.start();
      assert jdbcBinaryCacheStore.getConnectionFactory() instanceof ManagedConnectionFactory;
      return jdbcBinaryCacheStore;
   }


   public void testLoadFromFile() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/managed/binary-managed-connection-factory.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         CacheLoaderConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().loaders().cacheLoaders().get(0);
         assert firstCacheLoaderConfig != null;
         CacheLoaderConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().loaders().cacheLoaders().get(0);
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcBinaryCacheStoreConfiguration;
         assert secondCacheLoaderConfig instanceof JdbcBinaryCacheStoreConfiguration;
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
