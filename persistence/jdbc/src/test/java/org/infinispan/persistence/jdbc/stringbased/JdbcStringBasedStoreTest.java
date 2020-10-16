package org.infinispan.persistence.jdbc.stringbased;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;

import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.AbstractTableManager;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Tester class  for {@link JdbcStringBasedStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreTest")
public class JdbcStringBasedStoreTest extends BaseStoreTest {

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
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      storeBuilder.segmented(segmented);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      JdbcStringBasedStore stringBasedCacheStore = new JdbcStringBasedStore();
      stringBasedCacheStore.init(createContext(builder.build()));
      return stringBasedCacheStore;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false);
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .manageConnectionFactory(false)
            .dialect(DatabaseType.H2)
            .dbMajorVersion(1)
            .dbMinorVersion(4);

      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      storeBuilder.table()
            .createOnStart(false);

      JdbcStringBasedStore stringBasedCacheStore = new JdbcStringBasedStore();
      stringBasedCacheStore.init(createContext(builder.build()));

      // this will make sure that if a method like stop is called on the connection then it will barf an exception
      ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
      TableManager tableManager = mock(TableManager.class);
      TableManager.Metadata meta = new AbstractTableManager.MetadataImpl(Version.getVersionShort(), HashConfiguration.NUM_SEGMENTS.getDefaultValue());
      when(tableManager.metaTableExists(null)).thenReturn(true);
      when(tableManager.getMetadata(null)).thenReturn(meta);

      tableManager.start();
      assertNull(stringBasedCacheStore.getConnectionFactory());
      stringBasedCacheStore.setConnectionFactory(connectionFactory);
      stringBasedCacheStore.setTableManager(tableManager);
      stringBasedCacheStore.start();
      assertSame(stringBasedCacheStore.getConnectionFactory(), connectionFactory);

      //stop should be called even if this is an external
      reset(tableManager, connectionFactory);
      tableManager.stop();

      stringBasedCacheStore.stop();
   }

   @Override
   protected boolean storePurgesAllExpired() {
      // expiration listener is not called for the entries
      return false;
   }
}
