package org.infinispan.persistence.jdbc;

import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.SimpleConnectionFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.infinispan.test.jndi.DummyContextFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.ManagedConnectionFactoryTest")
public abstract class ManagedConnectionFactoryTest extends BaseStoreTest {

   private DummyDataSource ds;

   @BeforeClass
   public void bindDatasourceInJndi() throws Exception {
      System.setProperty(Context.INITIAL_CONTEXT_FACTORY, DummyContextFactory.class.getName());
      ds = new DummyDataSource();
      ds.start();
      InitialContext ic = new InitialContext();
      ic.bind(getDatasourceLocation(), ds);
      assert ic.lookup(getDatasourceLocation()) instanceof DummyDataSource;
   }

   public abstract String getDatasourceLocation();

   @AfterClass
   public void destroyDatasourceAndUnbind() throws NamingException {
      InitialContext ic = new InitialContext();
      ic.unbind(getDatasourceLocation());
      assert ic.lookup(getDatasourceLocation()) == null;
      ds.stop();
   }


   public static class DummyDataSource implements DataSource {

      private SimpleConnectionFactory simpleFactory;

      public void start() throws PersistenceException {

         JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
               .getDefaultCacheConfiguration(false)
               .persistence()
                  .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
         simpleFactory = new SimpleConnectionFactory();
         simpleFactory.start(UnitTestDatabaseManager.configureSimpleConnectionFactory(storeBuilder).create(), Thread.currentThread().getContextClassLoader());
      }

      public void stop() {
         simpleFactory.stop();
      }

      @Override
      public Connection getConnection() throws SQLException {
         try {
            return simpleFactory.getConnection();
         } catch (PersistenceException e) {
            throw new SQLException(e);
         }
      }

      @Override
      public Connection getConnection(String username, String password) throws SQLException {
         return getConnection();
      }

      @Override
      public PrintWriter getLogWriter() throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      @Override
      public void setLogWriter(PrintWriter out) throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      @Override
      public void setLoginTimeout(int seconds) throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      @Override
      public int getLoginTimeout() throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      @Override
      public <T> T unwrap(Class<T> iface) throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      @Override
      public boolean isWrapperFor(Class<?> iface) throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      public Logger getParentLogger() {
         throw new IllegalStateException("This should not be called!");
      }
   }
}
