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

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.util.Util;
import org.infinispan.loaders.jdbc.logging.Log;
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

   private static final Log log = LogFactory.getLog(SimpleConnectionFactory.class, Log.class);

   private String connectionUrl;
   private String userName;
   private String password;

   @Override
   public void start(ConnectionFactoryConfig config, ClassLoader classLoader) throws CacheLoaderException {
      loadDriver(config.getDriverClass(), classLoader);
      this.connectionUrl = config.getConnectionUrl();
      this.userName = config.getUserName();
      this.password = config.getPassword();
      if (log.isTraceEnabled()) {
         log.tracef("Starting connection %s", this);
      }
   }

   @Override
   public void stop() {
      //do nothing
   }

   @Override
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

   @Override
   public void releaseConnection(Connection conn) {
      try {
         conn.close();
      } catch (SQLException e) {
         log.failureClosingConnection(e);
      }
   }

   private void loadDriver(String driverClass, ClassLoader classLoader) throws CacheLoaderException {
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

   @Override
   public String toString() {
      return "SimpleConnectionFactory{" +
              "connectionUrl='" + connectionUrl + '\'' +
              ", userName='" + userName + '\'' +
              "} " + super.toString();
   }
}
