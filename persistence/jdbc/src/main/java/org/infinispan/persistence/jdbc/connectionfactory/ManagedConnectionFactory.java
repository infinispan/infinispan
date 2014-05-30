package org.infinispan.persistence.jdbc.connectionfactory;

import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.ManagedConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.sql.DataSource;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Connection factory that can be used when on managed environments, like application servers. It knows how to look into
 * the JNDI tree at a certain location (configurable) and delegate connection management to the DataSource. In order to
 * enable it one should set the following two properties in any Jdbc cache store:
 * <pre>
 *    <property name="connectionFactoryClass" value="ManagedConnectionFactory"/>
 *    <property name="datasourceJndiLocation" value="java:/ManagedConnectionFactoryTest/DS"/>
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 * @author Sanne Grinovero
 */
public class ManagedConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(ManagedConnectionFactory.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private volatile DataSource dataSource;

   @Override
   public void start(ConnectionFactoryConfiguration factoryConfiguration, ClassLoader classLoader) throws PersistenceException {
      final String datasourceName = extractDataSourceName(factoryConfiguration);
      final InitialContext ctx = getInitialContext();
      try {
         dataSource = (DataSource) ctx.lookup(datasourceName);
         if (dataSource == null) {
            throw log.connectionInJndiNotFound(datasourceName);
         }
         else if (trace) {
            log.tracef("Datasource lookup for %s succeeded: %b", datasourceName, dataSource);
         }
      }
      catch (NamingException e) {
         throw log.namingExceptionLookingUpConnection(datasourceName, e);
      }
      finally {
         try {
            ctx.close();
         }
         catch (NamingException e) {
            log.failedClosingNamingCtx(e);
         }
      }
   }

   private String extractDataSourceName(ConnectionFactoryConfiguration factoryConfiguration) {
      if (factoryConfiguration instanceof ManagedConnectionFactoryConfiguration) {
         ManagedConnectionFactoryConfiguration managedConfiguration = (ManagedConnectionFactoryConfiguration) factoryConfiguration;
         return managedConfiguration.jndiUrl();
      }
      else {
         throw new PersistenceException("FactoryConfiguration has to be an instance of ManagedConnectionFactoryConfiguration");
      }
   }

   private InitialContext getInitialContext() {
      try {
         return new InitialContext();
      }
      catch  (NamingException e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public void stop() {
   }

   @Override
   public Connection getConnection() throws PersistenceException {
      Connection connection;
      try {
         connection = dataSource.getConnection();
      } catch (SQLException e) {
         throw log.sqlFailureRetrievingConnection(e);
      }
      if (trace) {
         log.tracef("Connection checked out: %s", connection);
      }
      return connection;

   }

   @Override
   public void releaseConnection(Connection conn) {
      try {         
         if (conn != null) // Could be null if getConnection failed
            conn.close();
      } catch (SQLException e) {
         log.sqlFailureClosingConnection(conn, e);
      }
   }
}
