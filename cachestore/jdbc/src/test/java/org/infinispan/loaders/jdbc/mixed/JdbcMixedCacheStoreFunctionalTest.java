package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreFunctionalTest")
public class JdbcMixedCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      JdbcMixedStoreConfigurationBuilder store = loaders
         .addStore(JdbcMixedStoreConfigurationBuilder.class)
         .preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.binaryTable(), true);
      UnitTestDatabaseManager.buildTableManipulation(store.stringTable(), false);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return loaders;
   }

}
