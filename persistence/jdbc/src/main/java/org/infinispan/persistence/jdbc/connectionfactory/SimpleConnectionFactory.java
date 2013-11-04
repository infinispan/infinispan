package org.infinispan.persistence.jdbc.connectionfactory;

import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.commons.util.Util;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.SimpleConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Connection factory implementation that will create database connection on a per invocation basis. Not recommended in
 * production, {@link PooledConnectionFactory} or {@link ManagedConnectionFactory} should rather be used.
 *
 * @author Mircea.Markus@jboss.com
 */
public class SimpleConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(SimpleConnectionFactory.class, Log.class);

   private String connectionUrl;
   private String userName;
   private String password;
   private volatile int connectionCount = 0;

   @Override
   public void start(ConnectionFactoryConfiguration config, ClassLoader classLoader) throws PersistenceException {

      SimpleConnectionFactoryConfiguration factoryConfiguration;
      if (config instanceof SimpleConnectionFactoryConfiguration) {
         factoryConfiguration = (SimpleConnectionFactoryConfiguration) config;
      }
      else {
         throw new PersistenceException("ConnectionFactoryConfiguration has to be an instance of " +
               "SimpleConnectionFactoryConfiguration.");
      }

      loadDriver(factoryConfiguration.driverClass(), classLoader);
      this.connectionUrl = factoryConfiguration.connectionUrl();
      this.userName = factoryConfiguration.username();
      this.password = factoryConfiguration.password();
      if (log.isTraceEnabled()) {
         log.tracef("Starting connection %s", this);
      }
   }

   @Override
   public void stop() {
      //do nothing
   }

   @Override
   public Connection getConnection() throws PersistenceException {
      try {
         Connection connection = DriverManager.getConnection(connectionUrl, userName, password);
         if (connection == null)
            throw new PersistenceException("Received null connection from the DriverManager!");
         connectionCount++;
         return connection;
      } catch (SQLException e) {
         throw new PersistenceException("Could not obtain a new connection", e);
      }
   }

   @Override
   public void releaseConnection(Connection conn) {
      try {
         if (conn!=null) {
            conn.close();
            connectionCount--;
         }
      } catch (SQLException e) {
         log.failureClosingConnection(e);
      }
   }

   private void loadDriver(String driverClass, ClassLoader classLoader) throws PersistenceException {
      if (log.isTraceEnabled()) log.tracef("Attempting to load driver %s", driverClass);
      Util.getInstance(driverClass, classLoader);
   }

   public String getConnectionUrl() {
      return connectionUrl;
   }

   public String getUserName() {
      return userName;
   }

   public String getPassword() {
      return password;
   }

   public int getConnectionCount() {
      return connectionCount;
   }

   @Override
   public String toString() {
      return "SimpleConnectionFactory{" +
              "connectionUrl='" + connectionUrl + '\'' +
              ", userName='" + userName + '\'' +
              "} " + super.toString();
   }
}
