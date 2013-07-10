package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedCacheStoreConfigurationBuilder;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStore2Test")
public class JdbcMixedCacheStore2Test extends BaseCacheStoreTest {
   @Override
   protected CacheStore createCacheStore() throws Exception {

      JdbcMixedCacheStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
               .loaders()
                  .addLoader(JdbcMixedCacheStoreConfigurationBuilder.class)
                     .purgeSynchronously(true);

      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.stringTable(), false);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.binaryTable(), true);
      JdbcMixedCacheStore cacheStore = new JdbcMixedCacheStore();
      cacheStore.init(storeBuilder.create(), getCache(), getMarshaller());
      cacheStore.start();
      return cacheStore;
   }
}
