package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreTest;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStore2Test")
public class JdbcMixedCacheStore2Test extends BaseCacheStoreTest {

   private EmbeddedCacheManager cacheManager;
   private Cache<Object,Object> cache;

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {

      ConfigurationBuilder cc = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcMixedStoreConfigurationBuilder storeBuilder = cc
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.stringTable(), false);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.binaryTable(), true);

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
      TestingUtil.killCacheManagers(cacheManager);
   }
}
