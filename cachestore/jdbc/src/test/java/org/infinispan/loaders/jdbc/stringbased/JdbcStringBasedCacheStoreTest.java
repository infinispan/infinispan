package org.infinispan.loaders.jdbc.stringbased;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import org.infinispan.CacheImpl;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.keymappers.UnsupportedKeyTypeException;
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
      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tm = UnitTestDatabaseManager.buildStringTableManipulation();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(connectionFactoryConfig, tm);
      config.setPurgeSynchronously(true);
      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      stringBasedCacheStore.init(config, getCache(), getMarshaller());
      stringBasedCacheStore.start();
      return stringBasedCacheStore;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(false);
      config.setCreateTableOnStart(false);
      stringBasedCacheStore.init(config, getCache(), getMarshaller());
      stringBasedCacheStore.start();
      assert stringBasedCacheStore.getConnectionFactory() == null;

      // this will make sure that if a method like stop is called on the connection then it will barf an exception
      ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
      TableManipulation tableManipulation = mock(TableManipulation.class);
      config.setTableManipulation(tableManipulation);

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
