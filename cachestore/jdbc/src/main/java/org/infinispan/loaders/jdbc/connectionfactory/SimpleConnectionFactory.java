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

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Connection factory implementation that will create database connection on a per invocation basis. Not recommended in
 * production, {@link org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory} should rather be used.
 *
 * @author Mircea.Markus@jboss.com
 */
public class SimpleConnectionFactory extends ConnectionFactory {

   private static Log log = LogFactory.getLog(SimpleConnectionFactory.class);

   private String connectionUrl;
   private String userName;
   private String password;

   public void start(ConnectionFactoryConfig config) throws CacheLoaderException {
      loadDriver(config.getDriverClass());
      this.connectionUrl = config.getConnectionUrl();
      this.userName = config.getUserName();
      this.password = config.getPassword();
      if (log.isTraceEnabled()) {
         log.trace("Starting connection " + this);
      }
   }

   public void stop() {
      //do nothing
   }

   public Connection getConnection() throws CacheLoaderException {
      try {
         Connection connection = DriverManager.getConnection(connectionUrl, userName, password);
         if (connection == null)
            throw new CacheLoaderException("Received null connection from the DriverManager!");
         return connection;
      } catch (SQLException e) {
         throw new CacheLoaderException("Could not obtain a new connection", e);
      }
   }

   public void releaseConnection(Connection conn) {
      try {
         conn.close();
      } catch (SQLException e) {
         log.warn("Failure while closing the connection to the database ", e);
      }
   }

   private void loadDriver(String driverClass) throws CacheLoaderException {
      try {
         if (log.isTraceEnabled()) {
            log.trace("Attempting to load driver " + driverClass);
         }
         Util.getInstance(driverClass);
      }
      catch (Throwable th) {
         String message = "Failed loading driver with class: '" + driverClass + "'";
         log.error(message, th);
         throw new CacheLoaderException(message, th);
      }
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

   @Override
   public String toString() {
      return "SimpleConnectionFactory{" +
            "connectionUrl='" + connectionUrl + '\'' +
            ", userName='" + userName + '\'' +
            "} " + super.toString();
   }
}
