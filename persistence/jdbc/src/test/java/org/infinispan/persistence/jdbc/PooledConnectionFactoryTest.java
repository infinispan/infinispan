package org.infinispan.persistence.jdbc;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.C3P0ConnectionPool;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionPool;
import org.infinispan.persistence.jdbc.connectionfactory.HikariConnectionPool;
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
      System.setProperty("infinispan.jdbc.c3p0.force", "false");
   }

   @Test
   public void testHikariValuesNoOverrides() throws Exception {
      testValuesNoOverrides();
   }

   @Test
   public void testC3P0ValuesNoOverrides() throws Exception {
      System.setProperty("infinispan.jdbc.c3p0.force", "true");
      testValuesNoOverrides();
   }

   private void testValuesNoOverrides() throws Exception {
      storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      factoryBuilder = UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      ConnectionFactoryConfiguration factoryConfiguration = factoryBuilder.create();
      factory.start(factoryConfiguration, Thread.currentThread().getContextClassLoader());

      int hardcodedMaxPoolSize = factory.getMaxPoolSize();
      Set<Connection> connections = new HashSet<>();
      for (int i = 0; i < hardcodedMaxPoolSize; i++) {
         connections.add(factory.getConnection());
      }
      assert connections.size() == hardcodedMaxPoolSize;
      assert factory.getNumBusyConnectionsAllUsers() == hardcodedMaxPoolSize;
      for (Connection conn : connections) {
         conn.close();
      }
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 2000) {
         if (factory.getNumBusyConnectionsAllUsers() == 0)
            break;
      }
      //this must happen eventually
      assert factory.getNumBusyConnectionsAllUsers() == 0;
   }

   @Test(expectedExceptions = PersistenceException.class)
   public void testC3PONoDriverClassFound() throws Exception {
      System.setProperty("infinispan.jdbc.c3p0.force", "true");
      testNoDriverClassFound();
   }

   @Test(expectedExceptions = PersistenceException.class)
   public void testHikariCPNoDriverClassFound() throws Exception {
      testNoDriverClassFound();
   }

   private void testNoDriverClassFound() throws Exception {
      storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      factoryBuilder = UnitTestDatabaseManager.configureBrokenConnectionFactory(storeBuilder);
      ConnectionFactoryConfiguration factoryConfiguration = factoryBuilder.create();
      factory.start(factoryConfiguration, Thread.currentThread().getContextClassLoader());
   }

   @Test
   public void testHikariCPLoaded() throws Exception {
      testConnectionPoolLoaded(HikariConnectionPool.class);
   }

   @Test
   public void testC3POLoaded() throws Exception {
      System.setProperty("infinispan.jdbc.c3p0.force", "true");
      testConnectionPoolLoaded(C3P0ConnectionPool.class);
   }

   private void testConnectionPoolLoaded(Class connectionPoolType) throws Exception {
      storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      factoryBuilder = UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      ConnectionFactoryConfiguration factoryConfiguration = factoryBuilder.create();
      factory.start(factoryConfiguration, Thread.currentThread().getContextClassLoader());

      Field field = factory.getClass().getDeclaredField("connectionPool");
      field.setAccessible(true);
      ConnectionPool connectionPool = (ConnectionPool) field.get(factory);
      assert connectionPool != null;
      assert connectionPoolType.isInstance(connectionPool);
   }
}
