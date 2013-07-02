package org.infinispan.loaders.jdbc;

import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.SimpleConnectionFactory;
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
@Test(groups = "functional", testName = "loaders.jdbc.ManagedConnectionFactoryTest")
public abstract class ManagedConnectionFactoryTest extends BaseCacheStoreTest {

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


   @Override
   public void testConcurrency() throws Exception {
      //this is a long lasting method and this test is only to make sure the connection is properly fetched
   }

   public static class DummyDataSource implements DataSource {

      private SimpleConnectionFactory simpleFactory;

      public void start() throws CacheLoaderException {
         ConnectionFactoryConfig config = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
         simpleFactory = new SimpleConnectionFactory();
         simpleFactory.start(config, Thread.currentThread().getContextClassLoader());
      }

      public void stop() {
         simpleFactory.stop();
      }

      public Connection getConnection() throws SQLException {
         try {
            return simpleFactory.getConnection();
         } catch (CacheLoaderException e) {
            throw new SQLException(e);
         }
      }

      public Connection getConnection(String username, String password) throws SQLException {
         return getConnection();
      }

      public PrintWriter getLogWriter() throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      public void setLogWriter(PrintWriter out) throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      public void setLoginTimeout(int seconds) throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      public int getLoginTimeout() throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      public <T> T unwrap(Class<T> iface) throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      public boolean isWrapperFor(Class<?> iface) throws SQLException {
         throw new IllegalStateException("This should not be called!");
      }

      public Logger getParentLogger() {
         throw new IllegalStateException("This should not be called!");
      }
   }
}
