package org.infinispan.persistence.jdbc;

import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

/**
 * Tester class for {@link org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 */
@Test(groups = "functional", testName = "persistence.jdbc.PooledConnectionFactoryTest")
public class PooledConnectionFactoryTest {

   private PooledConnectionFactory factory;
   private ConnectionFactoryConfigurationBuilder<?> factoryBuilder;

   @AfterMethod
   public void destroyFacotry() {
      factory.stop();
   }

   @Test(groups = "unstable", description = "See ISPN-3522")
   public void testValuesNoOverrides() throws Exception {

      JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);

      factoryBuilder = UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);

      factory = new PooledConnectionFactory();

      ConnectionFactoryConfiguration factoryConfiguration = factoryBuilder.create();
      factory.start(factoryConfiguration, Thread.currentThread().getContextClassLoader());

      int hadcodedMaxPoolSize = factory.getPooledDataSource().getMaxPoolSize();
      Set<Connection> connections = new HashSet<Connection>();
      for (int i = 0; i < hadcodedMaxPoolSize; i++) {
         connections.add(factory.getConnection());
      }
      assert connections.size() == hadcodedMaxPoolSize;
      assert factory.getPooledDataSource().getNumBusyConnections() == hadcodedMaxPoolSize;
      for (Connection conn : connections) {
         conn.close();
      }
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 2000) {
         if (factory.getPooledDataSource().getNumBusyConnections() == 0)
            break;
      }
      //this must happen eventually
      assert factory.getPooledDataSource().getNumBusyConnections() == 0;
   }

   @Test(expectedExceptions = PersistenceException.class)
   public void testNoDriverClassFound() throws Exception {

      JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);

      factoryBuilder = UnitTestDatabaseManager.configureBrokenConnectionFactory(storeBuilder);

      factory = new PooledConnectionFactory();

      ConnectionFactoryConfiguration factoryConfiguration = factoryBuilder.create();
      factory.start(factoryConfiguration, Thread.currentThread().getContextClassLoader());
   }

}
