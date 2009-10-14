package org.infinispan.loaders.jdbc.connectionfactory;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.sql.DataSource;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Connection factory that can be used when on managed environments, like applicatons servers. It knows how to look into
 * the JNDI tree at a certain location (configurable) and delegate connection management to the DataSource. In order to
 * enable ti one should set the following two properties in any Jdbc cache store:
 * <pre>
 *   <property name="connectionFactoryClass"
 *                        value="org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory"/>
 *    <property name="datasourceJndiLocation" value="java:/ManagedConnectionFactoryTest/DS"/>
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 */
public class ManagedConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(ManagedConnectionFactory.class);
   private static final boolean trace = log.isTraceEnabled();

   private DataSource dataSource;

   public void start(ConnectionFactoryConfig config) throws CacheLoaderException {
      InitialContext ctx = null;
      String datasourceName = config.getDatasourceJndiLocation();
      try {
         ctx = new InitialContext();
         dataSource = (DataSource) ctx.lookup(datasourceName);
         if (trace) {
            log.trace("Datasource lookup for " + datasourceName + " succeded: " + dataSource);
         }
         if (dataSource == null) {
            String msg = "Could not find a connection in jndi under the name '" + datasourceName + "'";
            log.error(msg);
            throw new CacheLoaderException(msg);
         }
      }
      catch (NamingException e) {
         log.error("Could not lookup connection with datasource " + datasourceName, e);
         throw new CacheLoaderException(e);
      }
      finally {
         if (ctx != null) {
            try {
               ctx.close();
            }
            catch (NamingException e) {
               log.warn("Failed to close naming context.", e);
            }
         }
      }
   }

   public void stop() {
   }

   public Connection getConnection() throws CacheLoaderException {
      Connection connection;
      try {
         connection = dataSource.getConnection();
      } catch (SQLException e) {
         log.error(e);
         throw new CacheLoaderException(e);
      }
      if (trace) {
         log.trace("Connection checked out: " + connection);
      }
      return connection;

   }

   public void releaseConnection(Connection conn) {
      try {
         conn.close();
      } catch (SQLException e) {
         log.warn("Issues while closing connection " + conn, e);
      }
   }
}
