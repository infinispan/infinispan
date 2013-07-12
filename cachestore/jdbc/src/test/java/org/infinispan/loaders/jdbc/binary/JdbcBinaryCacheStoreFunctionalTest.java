package org.infinispan.loaders.jdbc.binary;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryCacheStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * JdbcBinaryCacheStoreFunctionalTest.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "loaders.jdbc.binary.JdbcBinaryCacheStoreFunctionalTest")
public class JdbcBinaryCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @Override
   protected LoadersConfigurationBuilder createCacheStoreConfig(LoadersConfigurationBuilder loaders) {
      JdbcBinaryCacheStoreConfigurationBuilder store = loaders
         .addStore(JdbcBinaryCacheStoreConfigurationBuilder.class)
         .purgeSynchronously(true);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
      return loaders;
   }
}
