package org.infinispan.loaders.jdbc.stringbased;

import static org.easymock.classextension.EasyMock.*;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.infinispan.CacheDelegate;
import org.testng.annotations.Test;

/**
 * Tester class  for {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.stringbased.JdbcStringBasedCacheStoreTest")
public class JdbcStringBasedCacheStoreTest extends BaseCacheStoreTest {

   protected CacheStore createCacheStore() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      TableManipulation tm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(connectionFactoryConfig, tm);
      JdbcStringBasedCacheStore jdbcBucketCacheStore = new JdbcStringBasedCacheStore();
      jdbcBucketCacheStore.init(config, new CacheDelegate("aName"), getMarshaller());
      jdbcBucketCacheStore.start();
      return jdbcBucketCacheStore;
   }

   public void testNotCreateConnectionFactory() throws Exception {
      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(false);
      config.setCreateTableOnStart(false);
      stringBasedCacheStore.init(config, new CacheDelegate("otherName"), getMarshaller());
      stringBasedCacheStore.start();
      assert stringBasedCacheStore.getConnectionFactory() == null;

      // this will make sure that if a method like stop is called on the connection then it will barf an exception 
      ConnectionFactory connectionFactory = createMock(ConnectionFactory.class);
      TableManipulation tableManipulation = createMock(TableManipulation.class);
      config.setTableManipulation(tableManipulation);

      tableManipulation.start(connectionFactory);
      tableManipulation.setCacheName("otherName");
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
