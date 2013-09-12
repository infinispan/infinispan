package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringParallelIterationTest")
public class JdbcStringParallelIterationTest extends ParallelIterationTest {

   @Override
   protected int numThreads() {
      return 1;
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder =
            cb.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);
   }

}
