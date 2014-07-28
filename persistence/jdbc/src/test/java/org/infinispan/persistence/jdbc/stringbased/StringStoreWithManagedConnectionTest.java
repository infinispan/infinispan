package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
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
@Test (groups = "functional", testName = "persistence.jdbc.stringbased.StringStoreWithManagedConnectionTest")
public class StringStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class);

      storeBuilder.dataSource()
            .jndiUrl(getDatasourceLocation());
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);

      JdbcStringBasedStore stringBasedCacheStore = new JdbcStringBasedStore();
      stringBasedCacheStore.init(createContext(builder.build()));
      return stringBasedCacheStore;
   }

   public void testLoadFromFile() throws Exception {
      TestingUtil.withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/managed/str-managed-connection-factory.xml"), true) {
         @Override
         public void call() {
            Cache<String, String> first = cm.getCache("first");
            Cache<String, String> second = cm.getCache("second");

            StoreConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().persistence().stores().get(0);
            assertNotNull(firstCacheLoaderConfig);
            assertTrue(firstCacheLoaderConfig instanceof JdbcStringBasedStoreConfiguration);
            StoreConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().persistence().stores().get(0);
            assertNotNull(secondCacheLoaderConfig);
            assertTrue(secondCacheLoaderConfig instanceof JdbcStringBasedStoreConfiguration);
            JdbcStringBasedStore loader = (JdbcStringBasedStore) TestingUtil.getFirstLoader(first);
            assertTrue(loader.getConnectionFactory() instanceof ManagedConnectionFactory);
         }
      });
   }

   @Override
   public String getDatasourceLocation() {
      return "java:/StringStoreWithManagedConnectionTest/DS";
   }

   @Override
   @Test(expectedExceptions = UnsupportedKeyTypeException.class)
   public void testLoadAndStoreMarshalledValues() throws PersistenceException {
      super.testLoadAndStoreMarshalledValues();
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }
}
