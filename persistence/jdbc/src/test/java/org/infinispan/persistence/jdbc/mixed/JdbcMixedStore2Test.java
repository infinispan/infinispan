package org.infinispan.persistence.jdbc.mixed;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.jdbc.mixed.JdbcMixedStore2Test")
public class JdbcMixedStore2Test extends BaseStoreTest {
   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {

      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcMixedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
            .addStore(JdbcMixedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.setDialect(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.stringTable(), false);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.binaryTable(), true);

      JdbcMixedStore jdbcMixed = new JdbcMixedStore();
      jdbcMixed.init(createContext(builder.build()));
      return jdbcMixed;
   }

   @Override
   protected boolean storePurgesAllExpired() {
      return false;
   }
}
