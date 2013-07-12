package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedCacheStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.stringbased.JdbcStringBasedCacheStoreFunctionalTest")
public class JdbcStringBasedCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected LoadersConfigurationBuilder createCacheStoreConfig(LoadersConfigurationBuilder loaders) {
      JdbcStringBasedCacheStoreConfigurationBuilder store = loaders
         .addStore(JdbcStringBasedCacheStoreConfigurationBuilder.class)
         .purgeSynchronously(true);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), false);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return loaders;
   }
}
