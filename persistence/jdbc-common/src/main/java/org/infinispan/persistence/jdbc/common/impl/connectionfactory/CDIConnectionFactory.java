package org.infinispan.persistence.jdbc.common.impl.connectionfactory;

import static org.infinispan.persistence.jdbc.common.logging.Log.PERSISTENCE;

import java.lang.annotation.Annotation;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.persistence.jdbc.common.configuration.CDIConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

import jakarta.enterprise.inject.spi.CDI;

/**
 * Connection factory that can be used when on CDI environments.
 */
public class CDIConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(CDIConnectionFactory.class, Log.class);

   private CDIConnectionFactoryConfiguration managedConfiguration;
   private volatile DataSource dataSource;


   @Override
   public void start(ConnectionFactoryConfiguration factoryConfiguration, ClassLoader classLoader) throws PersistenceException {
      managedConfiguration = (CDIConnectionFactoryConfiguration) factoryConfiguration;
   }

   private void initDataSource() {
      try {
         String datasourceName = managedConfiguration.name();
         if (datasourceName == null) {
            dataSource = CDI.current().select(DataSource.class).get();
         } else {
            Class<? extends Annotation> annotationClass = (Class<? extends Annotation>) ReflectionUtil.getClassForName(managedConfiguration.annotation(), Thread.currentThread().getContextClassLoader());
            Annotation annotation = Util.newInstanceOrNull(annotationClass, new Class[]{String.class}, datasourceName);
            dataSource = CDI.current().select(DataSource.class, annotation).get();
         }
         if (dataSource == null) {
            throw PERSISTENCE.connectionNotFound("CDI", datasourceName);
         }
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void stop() {
   }

   @Override
   public Connection getConnection() throws PersistenceException {
      if (dataSource == null)
         initDataSource();

      Connection connection;
      try {
         connection = dataSource.getConnection();
      } catch (SQLException e) {
         throw PERSISTENCE.sqlFailureRetrievingConnection(e);
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
