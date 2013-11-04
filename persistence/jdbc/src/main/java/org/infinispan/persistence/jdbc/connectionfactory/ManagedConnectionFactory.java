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
 */
public class ManagedConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(ManagedConnectionFactory.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private DataSource dataSource;

   @Override
   public void start(ConnectionFactoryConfiguration factoryConfiguration, ClassLoader classLoader) throws PersistenceException {
      InitialContext ctx = null;
      String datasourceName;
      if (factoryConfiguration instanceof ManagedConnectionFactoryConfiguration) {
         ManagedConnectionFactoryConfiguration managedConfiguration = (ManagedConnectionFactoryConfiguration)
               factoryConfiguration;
         datasourceName = managedConfiguration.jndiUrl();
      }
      else {
         throw new PersistenceException("FactoryConfiguration has to be an instance of " +
               "ManagedConnectionFactoryConfiguration");
      }
      try {
         ctx = new InitialContext();
         dataSource = (DataSource) ctx.lookup(datasourceName);
         if (trace) {
            log.tracef("Datasource lookup for %s succeeded: %b", datasourceName, dataSource);
         }
         if (dataSource == null) {
            log.connectionInJndiNotFound(datasourceName);
            throw new PersistenceException(String.format(
                  "Could not find a connection in jndi under the name '%s'", datasourceName));
         }
      }
      catch (NamingException e) {
         log.namingExceptionLookingUpConnection(datasourceName, e);
         throw new PersistenceException(e);
      }
      finally {
         if (ctx != null) {
            try {
               ctx.close();
            }
            catch (NamingException e) {
               log.failedClosingNamingCtx(e);
            }
         }
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
         log.sqlFailureRetrievingConnection(e);
         throw new PersistenceException("This might be related to https://jira.jboss.org/browse/ISPN-604", e);
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
