package org.infinispan.persistence.jdbc.binary;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.binary.BinaryStoreWithManagedConnectionTest")
public class BinaryStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcBinaryStoreConfigurationBuilder storeBuilder = builder
            .persistence()
               .addStore(JdbcBinaryStoreConfigurationBuilder.class);
      storeBuilder.dataSource().jndiUrl(getDatasourceLocation());
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), true);

      JdbcBinaryStore jdbcBinaryStore = new JdbcBinaryStore();
      jdbcBinaryStore.init(createContext(builder.build()));
      return jdbcBinaryStore;
   }

   public void testLoadFromFile() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/managed/binary-managed-connection-factory.xml"), true) {
         @Override
         public void call() {
            Cache<String, String> first = cm.getCache("first");
            Cache<String, String> second = cm.getCache("second");

            StoreConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().persistence().stores().get(0);
            assertNotNull(firstCacheLoaderConfig);
            StoreConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().persistence().stores().get(0);
            assertNotNull(secondCacheLoaderConfig);
            assertTrue(firstCacheLoaderConfig instanceof JdbcBinaryStoreConfiguration);
            assertTrue(secondCacheLoaderConfig instanceof JdbcBinaryStoreConfiguration);
            JdbcBinaryStore loader = (JdbcBinaryStore) TestingUtil.getFirstLoader(first);
            assertTrue(loader.getConnectionFactory() instanceof ManagedConnectionFactory);
         }
      });
   }

   @Override
   public String getDatasourceLocation() {
      return "java:/BinaryStoreWithManagedConnectionTest/DS";
   }

}
