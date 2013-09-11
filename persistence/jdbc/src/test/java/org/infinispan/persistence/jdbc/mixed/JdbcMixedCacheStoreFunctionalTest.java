package org.infinispan.persistence.jdbc.mixed;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreFunctionalTest;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.jdbc.mixed.JdbcMixedCacheStoreFunctionalTest")
public class JdbcMixedCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      JdbcMixedStoreConfigurationBuilder store = persistence
         .addStore(JdbcMixedStoreConfigurationBuilder.class)
         .preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.binaryTable(), true);
      UnitTestDatabaseManager.buildTableManipulation(store.stringTable(), false);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return persistence;
   }

}
