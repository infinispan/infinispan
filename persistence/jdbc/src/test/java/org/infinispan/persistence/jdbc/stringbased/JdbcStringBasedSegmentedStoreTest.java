package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.SegmentedStoreTest;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * Test to ensure that JDBC string based store works properly when segmented
 * @author wburns
 * @since 9.4
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedSegmentedStoreTest")
public class JdbcStringBasedSegmentedStoreTest extends SegmentedStoreTest {
   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder =
            cb.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      storeBuilder.segmented(true);
   }
}
