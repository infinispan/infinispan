package org.infinispan.persistence.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.jdbc.mixed.JdbcMixedStore2Test")
public class JdbcMixedStore2Test extends BaseStoreTest {

   private EmbeddedCacheManager cacheManager;
   private Cache<Object,Object> cache;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {

      ConfigurationBuilder cc = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcMixedStoreConfigurationBuilder storeBuilder = cc
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.setDialect(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.stringTable(), false);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.binaryTable(), true);

      cacheManager = TestCacheManagerFactory.createCacheManager(cc);
      cache = cacheManager.getCache();

      JdbcMixedStore jdbcMixed = TestingUtil.getFirstWriter(cache);

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
   public void tearDown() throws PersistenceException {
      TestingUtil.killCacheManagers(cacheManager);
   }
}
