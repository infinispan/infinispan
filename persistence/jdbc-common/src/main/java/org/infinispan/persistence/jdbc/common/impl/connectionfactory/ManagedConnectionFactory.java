package org.infinispan.persistence.jdbc.common.impl.connectionfactory;

import static org.infinispan.persistence.jdbc.common.logging.Log.PERSISTENCE;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.configuration.ManagedConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

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
         if (log.isTraceEnabled()) {
            log.tracef("Datasource lookup for %s succeeded: %b", datasourceName, dataSource);
         }
         if (dataSource == null) {
            PERSISTENCE.connectionInJndiNotFound(datasourceName);
            throw new PersistenceException(String.format(
                  "Could not find a connection in jndi under the name '%s'", datasourceName));
         }
      }
      catch (NamingException e) {
         PERSISTENCE.namingExceptionLookingUpConnection(datasourceName, e);
         throw new PersistenceException(e);
      }
      finally {
         if (ctx != null) {
            try {
               ctx.close();
            }
            catch (NamingException e) {
               PERSISTENCE.failedClosingNamingCtx(e);
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
         PERSISTENCE.sqlFailureRetrievingConnection(e);
         throw new PersistenceException("This might be related to https://jira.jboss.org/browse/ISPN-604", e);
      }
      if (log.isTraceEnabled()) {
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
         PERSISTENCE.sqlFailureClosingConnection(conn, e);
      }
   }
}
