package org.infinispan.loaders.jdbc.stringbased;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.keymappers.UnsupportedKeyTypeException;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * Tester class  for {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.stringbased.JdbcStringBasedCacheStoreTest")
public class JdbcStringBasedCacheStoreTest extends BaseCacheStoreTest {

   @Override
   protected CacheStore createCacheStore() throws Exception {
      JdbcStringBasedCacheStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
               .addLoader(JdbcStringBasedCacheStoreConfigurationBuilder.class)
                  .purgeSynchronously(true);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);
      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      stringBasedCacheStore.init(storeBuilder.create(), getCache(), getMarshaller());
      stringBasedCacheStore.start();
      return stringBasedCacheStore;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      JdbcStringBasedCacheStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .loaders()
            .addLoader(JdbcStringBasedCacheStoreConfigurationBuilder.class)
            .purgeSynchronously(true)
            .manageConnectionFactory(false);

      storeBuilder.table().createOnStart(false);

      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      stringBasedCacheStore.init(storeBuilder.create(), getCache(), getMarshaller());
      stringBasedCacheStore.start();
      assert stringBasedCacheStore.getConnectionFactory() == null;

      // this will make sure that if a method like stop is called on the connection then it will barf an exception
      ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
      TableManipulation tableManipulation = mock(TableManipulation.class);

      tableManipulation.start(connectionFactory);
      tableManipulation.setCacheName("otherName");

      stringBasedCacheStore.doConnectionFactoryInitialization(connectionFactory);

      //stop should be called even if this is an external
      reset(tableManipulation, connectionFactory);
      tableManipulation.stop();

      stringBasedCacheStore.stop();
   }

   @Override
   @Test(expectedExceptions = UnsupportedKeyTypeException.class)
   public void testLoadAndStoreMarshalledValues() throws CacheLoaderException {
      super.testLoadAndStoreMarshalledValues();
   }

}
