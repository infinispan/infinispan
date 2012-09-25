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

import java.sql.Driver;

import org.infinispan.config.ConfigurationException;

/**
 * SimpleConnectionFactoryBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class SimpleConnectionFactoryConfigurationBuilder<S extends AbstractJdbcCacheStoreConfigurationBuilder<?, S>> extends AbstractJdbcCacheStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<SimpleConnectionFactoryConfiguration> {

   private String connectionUrl;
   private String driverClass;
   private String username;
   private String password;

   public SimpleConnectionFactoryConfigurationBuilder(AbstractJdbcCacheStoreConfigurationBuilder<?, S> builder) {
      super(builder);
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> connectionUrl(String connectionUrl) {
      this.connectionUrl = connectionUrl;
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> driverClass(Class<? extends Driver> driverClass) {
      this.driverClass = driverClass.getName();
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> driverClass(String driverClass) {
      this.driverClass = driverClass;
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> username(String username) {
      this.username = username;
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> password(String password) {
      this.password = password;
      return this;
   }

   @Override
   public void validate() {
      if (connectionUrl == null) {
         throw new ConfigurationException("A connectionUrl has not been specified");
      }
   }

   @Override
   public SimpleConnectionFactoryConfiguration create() {
      return new SimpleConnectionFactoryConfiguration(connectionUrl, driverClass, username, password);
   }

   @Override
   public SimpleConnectionFactoryConfigurationBuilder<S> read(SimpleConnectionFactoryConfiguration template) {
      this.connectionUrl = template.connectionUrl();
      this.driverClass = template.driverClass();
      this.username = template.username();
      this.password = template.password();

      return this;
   }

}
