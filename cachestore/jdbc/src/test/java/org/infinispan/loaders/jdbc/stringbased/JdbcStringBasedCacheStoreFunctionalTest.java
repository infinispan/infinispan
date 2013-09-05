package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.stringbased.JdbcStringBasedCacheStoreFunctionalTest")
public class JdbcStringBasedCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      JdbcStringBasedStoreConfigurationBuilder store = loaders
         .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
         .preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), false);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return loaders;
   }

   @Override
   public void testPreloadAndExpiry() {
      super.testPreloadAndExpiry();    // TODO: Customise this generated block
   }
}
