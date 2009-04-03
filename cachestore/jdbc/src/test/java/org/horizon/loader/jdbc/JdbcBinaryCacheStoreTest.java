package org.horizon.loader.jdbc;

import org.horizon.loader.BaseCacheStoreTest;
import org.horizon.loader.CacheStore;
import org.horizon.loader.jdbc.binary.JdbcBinaryCacheStore;
import org.horizon.loader.jdbc.binary.JdbcBinaryCacheStoreConfig;
import org.horizon.loader.jdbc.connectionfactory.ConnectionFactory;
import org.horizon.loader.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.horizon.marshall.ObjectStreamMarshaller;
import org.testng.annotations.Test;
import static org.easymock.classextension.EasyMock.*;

/**
 * Tester class for {@link JdbcBinaryCacheStore}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loader.jdbc.JdbcBinaryCacheStoreTest")
public class JdbcBinaryCacheStoreTest extends BaseCacheStoreTest {



   protected CacheStore createCacheStore() throws Exception {
  


      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcBinaryCacheStoreConfig config = new JdbcBinaryCacheStoreConfig(connectionFactoryConfig, tm);
      JdbcBinaryCacheStore jdbcBucketCacheStore = new JdbcBinaryCacheStore();
      jdbcBucketCacheStore.init(config, null, new ObjectStreamMarshaller());
      jdbcBucketCacheStore.start();
      assert jdbcBucketCacheStore.getConnectionFactory() != null;
      return jdbcBucketCacheStore;


   }

   public void testNotCreateConnectionFactory() throws Exception {
      JdbcBinaryCacheStore jdbcBucketCacheStore = new JdbcBinaryCacheStore();
      JdbcBinaryCacheStoreConfig config = new JdbcBinaryCacheStoreConfig(false);
      config.setCreateTableOnStart(false);
      jdbcBucketCacheStore.init(config, null, new ObjectStreamMarshaller());
      jdbcBucketCacheStore.start();
      assert jdbcBucketCacheStore.getConnectionFactory() == null;

      /* this will make sure that if a method like stop is called on the connection then it will barf an exception */
      ConnectionFactory connectionFactory = createMock(ConnectionFactory.class);
      TableManipulation tableManipulation = createMock(TableManipulation.class);
      config.setTableManipulation(tableManipulation);

      tableManipulation.start(connectionFactory);
      replay(tableManipulation);
      jdbcBucketCacheStore.doConnectionFactoryInitialization(connectionFactory);
      verify(tableManipulation);

      //stop should be called even if this is an externally managed connection   
      reset(tableManipulation, connectionFactory);
      tableManipulation.stop();
      replay(tableManipulation, connectionFactory);
      jdbcBucketCacheStore.stop();
      verify(tableManipulation, connectionFactory);
   }
}
