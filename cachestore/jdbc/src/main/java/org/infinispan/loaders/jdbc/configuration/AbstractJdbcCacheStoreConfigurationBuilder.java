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
import org.infinispan.configuration.cache.AbstractLockSupportCacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;

public abstract class AbstractJdbcCacheStoreConfigurationBuilder<T extends AbstractJdbcCacheStoreConfiguration, S extends AbstractJdbcCacheStoreConfigurationBuilder<T, S>> extends AbstractLockSupportCacheStoreConfigurationBuilder<T, S> {
   protected String driverClass;
   protected String connectionUrl;
   protected String username;
   protected String password;
   protected String datasource;
   protected String connectionFactoryClass;

   public AbstractJdbcCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   public S driverClass(String driverClass) {
      this.driverClass = driverClass;
      return self();
   }

   public S driverClass(Class<? extends Driver> klass) {
      this.driverClass = klass.getName();
      return self();
   }

   public S connectionUrl(String connectionUrl) {
      this.connectionUrl = connectionUrl;
      return self();
   }

   public S username(String userName) {
      this.username = userName;
      return self();
   }

   public S password(String password) {
      this.password = password;
      return self();
   }

   public S datasource(String datasource) {
      this.datasource = datasource;
      return self();
   }

   public S connectionFactoryClass(String connectionFactoryClass) {
      this.connectionFactoryClass = connectionFactoryClass;
      return self();
   }

   public S connectionFactoryClass(Class<? extends ConnectionFactory> klass) {
      this.connectionFactoryClass = klass.getName();
      return self();
   }

   @Override
   public void validate() {
      super.validate();
      if (datasource != null && connectionUrl != null) {
         throw new ConfigurationException("Cannot specify both a datasource and a connection URL");
      }
   }

   /*
    * TODO: we should really be using inheritance here, but because of a javac bug it won't let me invoke
    * super.read() from subclasses complaining that abstract methods cannot be invoked. Will open a bug
    * and add the ID here
    */
   protected S readInternal(AbstractJdbcCacheStoreConfiguration template) {
      this.connectionFactoryClass(template.connectionFactoryClass());
      this.connectionUrl(template.connectionUrl());
      this.datasource(template.datasource());
      this.driverClass(template.driverClass());
      this.password(template.password());
      this.username(template.userName());
      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());

      return self();
   }
}
