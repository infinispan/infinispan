/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.util.logging.Log;
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
 */
public class PooledConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(PooledConnectionFactory.class);
   private ComboPooledDataSource pooledDataSource;

   @Override
   public void start(ConnectionFactoryConfig config) throws CacheLoaderException {
      logFileOverride();
      pooledDataSource = new ComboPooledDataSource();
      pooledDataSource.setProperties(new Properties());
      try {
         pooledDataSource.setDriverClass(config.getDriverClass()); //loads the jdbc driver
      } catch (PropertyVetoException e) {
         String message = "Error while instatianting JDBC driver: '" + config.getDriverClass();
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      }
      pooledDataSource.setJdbcUrl(config.getConnectionUrl());
      pooledDataSource.setUser(config.getUserName());
      pooledDataSource.setPassword(config.getPassword());
      if (log.isTraceEnabled()) {
         log.trace("Started connection factory with config: " + config);
      }
   }

   private void logFileOverride() {
      URL propsUrl = Thread.currentThread().getContextClassLoader().getResource("c3p0.properties");
      URL xmlUrl = Thread.currentThread().getContextClassLoader().getResource("c3p0-config.xml");
      if (log.isInfoEnabled() && propsUrl != null) {
         log.info("Found 'c3p0.properties' in classpath: " + propsUrl);
      }
      if (log.isInfoEnabled() && xmlUrl != null) {
         log.info("Found 'c3p0-config.xml' in classpath: " + xmlUrl);
      }
   }

   @Override
   public void stop() {
      try {
         DataSources.destroy(pooledDataSource);
         if (log.isTraceEnabled()) {
            log.debug("Successfully stopped PooledConnectionFactory.");
         }
      }
      catch (SQLException sqle) {
         log.warn("Could not destroy C3P0 connection pool: " + pooledDataSource, sqle);
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
            log.trace("DataSource before " + operation + " (NumBusyConnectionsAllUsers) : " + pooledDataSource.getNumBusyConnectionsAllUsers() + ", (NumConnectionsAllUsers) : " + pooledDataSource.getNumConnectionsAllUsers());
         } catch (SQLException e) {                                                                                                                
            log.warn("Unexpected", e);
         }
      }
   }

   private void logAfter(Connection connection, boolean checkout)  {
      if (log.isTraceEnabled()) {
         String operation = checkout ? "checkout" : "release";
         try {
            log.trace("DataSource after " + operation + " (NumBusyConnectionsAllUsers) : " + pooledDataSource.getNumBusyConnectionsAllUsers() + ", (NumConnectionsAllUsers) : " + pooledDataSource.getNumConnectionsAllUsers());
         } catch (SQLException e) {
            log.warn("Unexpected", e);
         }
         log.trace("Connection " + operation + " : " + connection);
      }
   }
}
