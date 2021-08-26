package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreFunctionalTest")
public class JdbcStringBasedStoreFunctionalTest extends BaseStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload) {
      JdbcStringBasedStoreConfigurationBuilder store = persistence
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.table());
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      modifyJdbcConfiguration(store);
      return persistence;
   }

   protected void modifyJdbcConfiguration(JdbcStringBasedStoreConfigurationBuilder builder) {

   }
}
