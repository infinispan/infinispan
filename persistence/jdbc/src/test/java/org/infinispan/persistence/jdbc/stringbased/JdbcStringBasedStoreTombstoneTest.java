package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseTombstonePersistenceTest;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.util.PersistenceMockUtil;
import org.testng.annotations.Test;

/**
 * Tests tombstone stored in {@link JdbcStringBasedStore}.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreTombstoneTest")
public class JdbcStringBasedStoreTombstoneTest extends BaseTombstonePersistenceTest {

   @Override
   protected WaitNonBlockingStore<String, String> getStore() {
      return wrapAndStart(new JdbcStringBasedStore<>(), createContext());
   }

   @Override
   protected boolean keysStreamContainsTombstones() {
      return false;
   }

   private InitializationContext createContext() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.LOCAL).hash().numSegments(numSegments());
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      storeBuilder.segmented(true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      return PersistenceMockUtil.createContext(getClass(), builder.build(), getMarshaller());
   }
}
