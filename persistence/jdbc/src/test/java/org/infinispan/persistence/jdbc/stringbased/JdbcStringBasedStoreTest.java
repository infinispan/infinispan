package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Tester class  for {@link JdbcStringBasedStore}.
 *
 * @author William Burns
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreTest")
public class JdbcStringBasedStoreTest extends BaseNonBlockingStoreTest {

   boolean segmented;

   public JdbcStringBasedStoreTest segmented(boolean segmented) {
      this.segmented = segmented;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new JdbcStringBasedStoreTest().segmented(false),
            new JdbcStringBasedStoreTest().segmented(true),
      };
   }

   @Override
   protected String parameters() {
      return "[" + segmented + "]";
   }

   @Override
   protected JdbcStringBasedStore createStore() throws Exception {
      return new JdbcStringBasedStore();
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = configurationBuilder
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      storeBuilder.segmented(segmented);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      return configurationBuilder.build();
   }
}
