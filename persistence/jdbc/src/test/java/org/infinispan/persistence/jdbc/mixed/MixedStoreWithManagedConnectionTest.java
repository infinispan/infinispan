package org.infinispan.persistence.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.mixed.MixedStoreWithManagedConnectionTest")
public class MixedStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {

      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcMixedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class);
      storeBuilder.dataSource().jndiUrl(getDatasourceLocation());
      UnitTestDatabaseManager.setDialect(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.stringTable(), false);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.binaryTable(), true);

      storeBuilder
            .binaryTable()
            .tableNamePrefix("BINARY_TABLE")
            .stringTable()
            .tableNamePrefix("STRINGS_TABLE");

      JdbcMixedStore jdbcMixed = new JdbcMixedStore();
      jdbcMixed.init(createContext(builder.build()));
      return jdbcMixed;
   }

   public void testLoadFromFile() throws Exception {
      TestingUtil.withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/managed/mixed-managed-connection-factory.xml"), true) {
         @Override
         public void call() {
            Cache<String, String> first = cm.getCache("first");
            Cache<String, String> second = cm.getCache("second");

            StoreConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().persistence().stores().get(0);
            assertNotNull(firstCacheLoaderConfig);
            StoreConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().persistence().stores().get(0);
            assertNotNull(secondCacheLoaderConfig);
            assertTrue(firstCacheLoaderConfig instanceof JdbcMixedStoreConfiguration);
            assertTrue(secondCacheLoaderConfig instanceof JdbcMixedStoreConfiguration);
            JdbcMixedStore loader = (JdbcMixedStore) TestingUtil.getFirstLoader(first);
            assertTrue(loader.getConnectionFactory() instanceof ManagedConnectionFactory);
         }
      });
   }

   @Override
   public String getDatasourceLocation() {
      return "java:/MixedStoreWithManagedConnectionTest/DS";
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }
}
