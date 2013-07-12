package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedCacheStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreFunctionalTest")
public class JdbcMixedCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected LoadersConfigurationBuilder createCacheStoreConfig(LoadersConfigurationBuilder loaders) {
      JdbcMixedCacheStoreConfigurationBuilder store = loaders
         .addStore(JdbcMixedCacheStoreConfigurationBuilder.class)
         .purgeSynchronously(true);
      UnitTestDatabaseManager.buildTableManipulation(store.binaryTable(), true);
      UnitTestDatabaseManager.buildTableManipulation(store.stringTable(), false);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return loaders;
   }

}
