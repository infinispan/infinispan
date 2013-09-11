package org.infinispan.persistence.jdbc.binary;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreFunctionalTest;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * JdbcBinaryCacheStoreFunctionalTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.binary.JdbcBinaryCacheStoreFunctionalTest")
public class JdbcBinaryCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      JdbcBinaryStoreConfigurationBuilder store = persistence
         .addStore(JdbcBinaryStoreConfigurationBuilder.class).preload(preload);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return persistence;
   }
}
