package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.loaders.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.mixed.MixedStoreWithManagedConnectionTest")
public class MixedStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   private EmbeddedCacheManager cacheManager;
   private Cache<Object,Object> cache;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {

      ConfigurationBuilder cc = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcMixedStoreConfigurationBuilder storeBuilder = cc
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class);
      storeBuilder.dataSource().jndiUrl(getDatasourceLocation());
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.stringTable(), false);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.binaryTable(), true);

      storeBuilder
            .binaryTable()
            .tableNamePrefix("BINARY_TABLE")
            .stringTable()
            .tableNamePrefix("STRINGS_TABLE");

      cacheManager = TestCacheManagerFactory.createCacheManager(cc);
      cache = cacheManager.getCache();

      JdbcMixedCacheStore jdbcMixed = (JdbcMixedCacheStore) TestingUtil.getFirstWriter(cache);

      csc = jdbcMixed.getConfiguration();
      return jdbcMixed;
   }


   @Override
   protected Cache getCache() {
      return cache;
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return cache.getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @AfterMethod
   @Override
   public void tearDown() throws CacheLoaderException {
      cache.clear();
      TestingUtil.killCacheManagers(cacheManager);
   }


   public void testLoadFromFile() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/managed/mixed-managed-connection-factory.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         StoreConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().persistence().stores().get(0);
         assert firstCacheLoaderConfig != null;
         StoreConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().persistence().stores().get(0);
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcMixedStoreConfiguration;
         assert secondCacheLoaderConfig instanceof JdbcMixedStoreConfiguration;
         JdbcMixedCacheStore loader = (JdbcMixedCacheStore) TestingUtil.getFirstLoader(first);
         assert loader.getConnectionFactory() instanceof ManagedConnectionFactory;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   @Override
   public String getDatasourceLocation() {
      return "java:/MixedStoreWithManagedConnectionTest/DS";
   }
}
