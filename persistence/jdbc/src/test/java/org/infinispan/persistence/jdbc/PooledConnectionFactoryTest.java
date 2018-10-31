package org.infinispan.persistence.jdbc;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester class for {@link org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 */
@Test(groups = "functional", testName = "persistence.jdbc.PooledConnectionFactoryTest")
public class PooledConnectionFactoryTest {

   private PooledConnectionFactory factory;
   private JdbcStringBasedStoreConfigurationBuilder storeBuilder;
   private ConnectionFactoryConfigurationBuilder<?> factoryBuilder;

   @BeforeMethod
   public void beforeMethod() {
      factory = new PooledConnectionFactory();
   }

   @AfterMethod
   public void destroyFactory() {
      factory.stop();
   }

   @Test
   public void testValuesNoOverrides() throws Exception {
      storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      // We load agroal.properties to enable metrics and ensure that property file loading works as expected
      factoryBuilder = storeBuilder.connectionPool().propertyFile("src/test/resources/configs/agroal.properties");
      ConnectionFactoryConfiguration factoryConfiguration = factoryBuilder.create();
      factory.start(factoryConfiguration, Thread.currentThread().getContextClassLoader());

      int hardcodedMaxPoolSize = factory.getMaxPoolSize();
      assert hardcodedMaxPoolSize == 20;
      Set<Connection> connections = new HashSet<>();
      for (int i = 0; i < hardcodedMaxPoolSize; i++) {
         connections.add(factory.getConnection());
      }
      assert connections.size() == hardcodedMaxPoolSize;
      assert factory.getActiveConnections() == hardcodedMaxPoolSize;
      for (Connection conn : connections) {
         conn.close();
      }
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 2000) {
         if (factory.getActiveConnections() == 0)
            break;
      }
      //this must happen eventually
      assert factory.getActiveConnections() == 0;
   }

   @Test(expectedExceptions = PersistenceException.class)
   public void testNoDriverClassFound() {
      storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      factoryBuilder = UnitTestDatabaseManager.configureBrokenConnectionFactory(storeBuilder);
      ConnectionFactoryConfiguration factoryConfiguration = factoryBuilder.create();
      factory.start(factoryConfiguration, Thread.currentThread().getContextClassLoader());
   }
}
