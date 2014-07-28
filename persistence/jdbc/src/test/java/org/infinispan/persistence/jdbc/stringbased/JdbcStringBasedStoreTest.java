package org.infinispan.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.keymappers.UnsupportedKeyTypeException;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tester class  for {@link JdbcStringBasedStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreTest")
public class JdbcStringBasedStoreTest extends BaseStoreTest {

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);
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
            .manageConnectionFactory(false);

      storeBuilder.table().createOnStart(false);

      JdbcStringBasedStore stringBasedCacheStore = new JdbcStringBasedStore();
      stringBasedCacheStore.init(createContext(builder.build()));
      stringBasedCacheStore.start();
      assertNull(stringBasedCacheStore.getConnectionFactory());

      // this will make sure that if a method like stop is called on the connection then it will barf an exception
      ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
      TableManipulation tableManipulation = mock(TableManipulation.class);

      tableManipulation.start(connectionFactory);
      tableManipulation.setCacheName("otherName");

      stringBasedCacheStore.initializeConnectionFactory(connectionFactory);

      //stop should be called even if this is an external
      reset(tableManipulation, connectionFactory);
      tableManipulation.stop();

      stringBasedCacheStore.stop();
   }

   @Override
   @Test(expectedExceptions = UnsupportedKeyTypeException.class)
   public void testLoadAndStoreMarshalledValues() throws PersistenceException {
      super.testLoadAndStoreMarshalledValues();
   }

   @Override
   protected boolean storePurgesAllExpired() {
      // expiration listener is not called for the entries
      return false;
   }
}
