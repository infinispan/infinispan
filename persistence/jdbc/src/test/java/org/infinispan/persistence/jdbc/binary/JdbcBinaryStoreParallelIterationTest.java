package org.infinispan.persistence.jdbc.binary;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "persistence.jdbc.binary.JdbcBinaryStoreParallelIterationTest")
public class JdbcBinaryStoreParallelIterationTest extends ParallelIterationTest {

   @Override
   protected int numThreads() {
      return KnownComponentNames.getDefaultThreads(KnownComponentNames.PERSISTENCE_EXECUTOR) + 1 /** caller's thread */;
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      JdbcBinaryStoreConfigurationBuilder store = cb.persistence()
            .addStore(JdbcBinaryStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(store.table(), true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(store);
   }
}
