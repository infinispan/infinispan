/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.BuiltBy;
import org.infinispan.loaders.jdbc.AbstractJdbcCacheStoreConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;

@BuiltBy(PooledConnectionFactoryConfigurationBuilder.class)
public class PooledConnectionFactoryConfiguration implements ConnectionFactoryConfiguration, LegacyConnectionFactoryAdaptor {
   private final String connectionUrl;
   private final String driverClass;
   private final String username;
   private final String password;

   PooledConnectionFactoryConfiguration(String connectionUrl, String driverClass, String username, String password) {
      this.connectionUrl = connectionUrl;
      this.driverClass = driverClass;
      this.username = username;
      this.password = password;
   }

   public String connectionUrl() {
      return connectionUrl;
   }

   public String driverClass() {
      return driverClass;
   }

   public String username() {
      return username;
   }

   public String password() {
      return password;
   }

   @Override
   public Class<? extends ConnectionFactory> connectionFactoryClass() {
      return PooledConnectionFactory.class;
   }

   @Override
   public void adapt(AbstractJdbcCacheStoreConfig config) {
      config.setConnectionFactoryClass(connectionFactoryClass().getName());
      config.setConnectionUrl(connectionUrl);
      config.setDriverClass(driverClass);
      config.setUserName(username);
      config.setPassword(password);
   }

   @Override
   public String toString() {
      return "PooledConnectionFactoryConfiguration [connectionUrl=" + connectionUrl + ", driverClass=" + driverClass + ", username=" + username + ", password=" + password + "]";
   }

}
