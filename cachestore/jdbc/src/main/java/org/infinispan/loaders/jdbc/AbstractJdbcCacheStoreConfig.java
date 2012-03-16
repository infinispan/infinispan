/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.jdbc;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;

/**
 * This is an abstract configuration class containing common elements for all JDBC cache store types.
 *
 * @author Manik Surtani
 * @version 4.1
 */
public abstract class AbstractJdbcCacheStoreConfig extends LockSupportCacheStoreConfig {

   protected ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();

   public void setConnectionFactoryClass(String connectionFactoryClass) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionFactoryClass(connectionFactoryClass);
   }

   public ConnectionFactoryConfig getConnectionFactoryConfig() {
      return connectionFactoryConfig;
   }

   /**
    * Jdbc connection string for connecting to the database. Mandatory.
    */
   public void setConnectionUrl(String connectionUrl) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setConnectionUrl(connectionUrl);
   }

   /**
    * Database username.
    */
   public void setUserName(String userName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setUserName(userName);
   }

   public void setDatasourceJndiLocation(String location) {
      testImmutability("datasourceJndiLocation");
      this.connectionFactoryConfig.setDatasourceJndiLocation(location);
   }

   /**
    * Database username's password.
    */
   public void setPassword(String password) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setPassword(password);
   }

   /**
    * The name of the driver used for connecting to the database. Mandatory, will be loaded before initiating the first
    * connection.
    */
   public void setDriverClass(String driverClassName) {
      testImmutability("connectionFactoryConfig");
      this.connectionFactoryConfig.setDriverClass(driverClassName);
   }

   @Override
   public AbstractJdbcCacheStoreConfig clone() {
      AbstractJdbcCacheStoreConfig result = (AbstractJdbcCacheStoreConfig) super.clone();
      result.connectionFactoryConfig = connectionFactoryConfig.clone();
      return result;
   }

   @Override
   public String toString() {
      return "AbstractJdbcCacheStoreConfig{" +
            "connectionFactoryConfig=" + connectionFactoryConfig +
            "} " + super.toString();
   }
}
