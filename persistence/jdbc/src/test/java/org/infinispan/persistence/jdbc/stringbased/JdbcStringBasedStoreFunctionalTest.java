package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreFunctionalTest")
public class JdbcStringBasedStoreFunctionalTest extends BaseStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      JdbcStringBasedStoreConfigurationBuilder store = persistence
         .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
         .preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), false);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return persistence;
   }
}
