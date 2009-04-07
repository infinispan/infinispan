package org.infinispan.loader.jdbc.stringbased;

import static org.easymock.classextension.EasyMock.*;
import org.infinispan.loader.BaseCacheStoreTest;
import org.infinispan.loader.CacheStore;
import org.infinispan.loader.jdbc.TableManipulation;
import org.infinispan.loader.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loader.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.marshall.ObjectStreamMarshaller;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * Tester class  for {@link org.infinispan.loader.jdbc.stringbased.JdbcStringBasedCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loader.jdbc.stringbased.JdbcStringBasedCacheStoreTest")
public class JdbcStringBasedCacheStoreTest extends BaseCacheStoreTest {

   protected CacheStore createCacheStore() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(connectionFactoryConfig, tm);
      JdbcStringBasedCacheStore jdbcBucketCacheStore = new JdbcStringBasedCacheStore();
      jdbcBucketCacheStore.init(config, null, new ObjectStreamMarshaller());
      jdbcBucketCacheStore.start();
      return jdbcBucketCacheStore;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(false);
      config.setCreateTableOnStart(false);
      stringBasedCacheStore.init(config, null, new ObjectStreamMarshaller());
      stringBasedCacheStore.start();
      assert stringBasedCacheStore.getConnectionFactory() == null;

      // this will make sure that if a method like stop is called on the connection then it will barf an exception 
      ConnectionFactory connectionFactory = createMock(ConnectionFactory.class);
      TableManipulation tableManipulation = createMock(TableManipulation.class);
      config.setTableManipulation(tableManipulation);

      tableManipulation.start(connectionFactory);
      replay(tableManipulation);
      stringBasedCacheStore.doConnectionFactoryInitialization(connectionFactory);
      verify(tableManipulation);

      //stop should be called even if this is an external
      reset(tableManipulation, connectionFactory);
      tableManipulation.stop();
      replay(tableManipulation, connectionFactory);
      stringBasedCacheStore.stop();
      verify(tableManipulation, connectionFactory);
   }
}
