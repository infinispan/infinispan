package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.loaders.keymappers.UnsupportedKeyTypeException;
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
@Test (groups = "functional", testName = "loaders.jdbc.stringbased.StringStoreWithManagedConnectionTest")
public class StringStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   @Override
   protected CacheStore createCacheStore() throws Exception {
      JdbcStringBasedCacheStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
               .addLoader(JdbcStringBasedCacheStoreConfigurationBuilder.class)
               .purgeSynchronously(true);

      storeBuilder.dataSource()
            .jndiUrl(getDatasourceLocation());
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);

      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      stringBasedCacheStore.init(storeBuilder.create(), getCache(), getMarshaller());
      stringBasedCacheStore.start();
      return stringBasedCacheStore;
   }

   public void testLoadFromFile() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/managed/str-managed-connection-factory.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         CacheLoaderConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().loaders().cacheLoaders().get(0);
         assert firstCacheLoaderConfig != null;
         CacheLoaderConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().loaders().cacheLoaders().get(0);
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcStringBasedCacheStoreConfiguration;
         assert secondCacheLoaderConfig instanceof JdbcStringBasedCacheStoreConfiguration;
         CacheLoaderManager cacheLoaderManager = first.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
         JdbcStringBasedCacheStore loader = (JdbcStringBasedCacheStore) cacheLoaderManager.getCacheLoader();
         assert loader.getConnectionFactory() instanceof ManagedConnectionFactory;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   @Override
   public String getDatasourceLocation() {
      return "java:/StringStoreWithManagedConnectionTest/DS";
   }

   @Override
   @Test(expectedExceptions = UnsupportedKeyTypeException.class)
   public void testLoadAndStoreMarshalledValues() throws CacheLoaderException {
      super.testLoadAndStoreMarshalledValues();
   }

}
