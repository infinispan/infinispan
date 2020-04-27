package org.infinispan.persistence.jdbc;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.xsite.irac.persistence.BaseIracPersistenceTest;
import org.testng.annotations.Test;

/**
 * Tests if the IRAC metadata is properly stored and retrieved from a {@link JdbcStringBasedStore}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.IracJDBCStoreTest")
public class IracJDBCStoreTest extends BaseIracPersistenceTest<String> {

   public IracJDBCStoreTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @Override
   protected void configure(ConfigurationBuilder builder) {
      JdbcStringBasedStoreConfigurationBuilder jdbcBuilder = builder.persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(jdbcBuilder.table());
      UnitTestDatabaseManager.configureUniqueConnectionFactory(jdbcBuilder);
   }
}
