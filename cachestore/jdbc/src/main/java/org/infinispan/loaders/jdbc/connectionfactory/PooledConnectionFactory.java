/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.jdbc.connectionfactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.JdbcUtil;
import org.infinispan.loaders.jdbc.logging.Log;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.logging.LogFactory;

import java.beans.PropertyVetoException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Pooled connection factory based on C3P0. For a complete configuration reference, look <a
 * href="http://www.mchange.com/projects/c3p0/index.html#configuration">here</a>. The connection pool can be configured
 * in various ways, as described <a href="http://www.mchange.com/projects/c3p0/index.html#configuration_files">here</a>.
 * The simplest way is by having an <tt>c3p0.properties</tt> file in the classpath. If no such file is found, default,
 * hardcoded values will be used.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 */
public class PooledConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(PooledConnectionFactory.class, Log.class);
   private ComboPooledDataSource pooledDataSource;

   @Override
   public void start(ConnectionFactoryConfig config, ClassLoader classLoader) throws CacheLoaderException {
      logFileOverride(classLoader);
      pooledDataSource = new ComboPooledDataSource();
      pooledDataSource.setProperties(new Properties());
      try {
         /* Since c3p0 does not throw an exception when it fails to load a driver we attempt to do so here
          * Also, c3p0 does not allow specifying a custom classloader, so use c3p0's
          */
         Class.forName(config.getDriverClass(), true, ComboPooledDataSource.class.getClassLoader());
         pooledDataSource.setDriverClass(config.getDriverClass()); //loads the jdbc driver
      } catch (Exception e) {
         log.errorInstantiatingJdbcDriver(config.getDriverClass(), e);
         throw new CacheLoaderException(String.format(
               "Error while instatianting JDBC driver: '%s'", config.getDriverClass()), e);
      }
      pooledDataSource.setJdbcUrl(config.getConnectionUrl());
      pooledDataSource.setUser(config.getUserName());
      pooledDataSource.setPassword(config.getPassword());
      if (log.isTraceEnabled()) {
         log.tracef("Started connection factory with config: %s", config);
      }
   }

   private void logFileOverride(ClassLoader classLoader) {
      URL propsUrl = FileLookupFactory.newInstance().lookupFileLocation("c3p0.properties", classLoader);
      URL xmlUrl = FileLookupFactory.newInstance().lookupFileLocation("c3p0-config.xml", classLoader);
      if (log.isDebugEnabled() && propsUrl != null) {
         log.debugf("Found 'c3p0.properties' in classpath: %s", propsUrl);
      }
      if (log.isDebugEnabled() && xmlUrl != null) {
         log.debugf("Found 'c3p0-config.xml' in classpath: %s", xmlUrl);
      }
   }

   @Override
   public void stop() {
      try {
         DataSources.destroy(pooledDataSource);
         if (log.isDebugEnabled()) {
            log.debug("Successfully stopped PooledConnectionFactory.");
         }
      }
      catch (SQLException sqle) {
         log.couldNotDestroyC3p0ConnectionPool(pooledDataSource!=null?pooledDataSource.toString():null, sqle);
      }
   }

   @Override
   public Connection getConnection() throws CacheLoaderException {
      try {
         logBefore(true);
         Connection connection = pooledDataSource.getConnection();
         logAfter(connection, true);
         return connection;
      } catch (SQLException e) {
         throw new CacheLoaderException("Failed obtaining connection from PooledDataSource", e);
      }
   }

   @Override
   public void releaseConnection(Connection conn) {
      logBefore(false);
      JdbcUtil.safeClose(conn);
      logAfter(conn, false);
   }

   public ComboPooledDataSource getPooledDataSource() {
      return pooledDataSource;
   }

   private void logBefore(boolean checkout) {
      if (log.isTraceEnabled()) {
         String operation = checkout ? "checkout" : "release";
         try {
            log.tracef("DataSource before %s (NumBusyConnectionsAllUsers) : %d, (NumConnectionsAllUsers) : %d",
                       operation, pooledDataSource.getNumBusyConnectionsAllUsers(), pooledDataSource.getNumConnectionsAllUsers());
         } catch (SQLException e) {
            log.sqlFailureUnexpected(e);
         }
      }
   }

   private void logAfter(Connection connection, boolean checkout)  {
      if (log.isTraceEnabled()) {
         String operation = checkout ? "checkout" : "release";
         try {
            log.tracef("DataSource after %s (NumBusyConnectionsAllUsers) : %d, (NumConnectionsAllUsers) : %d",
                      operation, pooledDataSource.getNumBusyConnectionsAllUsers(), pooledDataSource.getNumConnectionsAllUsers());
         } catch (SQLException e) {
            log.sqlFailureUnexpected(e);
         }
         log.tracef("Connection %s : %s", operation, connection);
      }
   }
}
