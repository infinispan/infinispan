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
import org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;

public abstract class AbstractJdbcCacheStoreConfigurationBuilder<T extends AbstractJdbcCacheStoreConfiguration, S extends AbstractJdbcCacheStoreConfigurationBuilder<T, S>>
      extends AbstractLockSupportCacheStoreConfigurationBuilder<T, S> {
   protected String driverClass;
   protected String connectionUrl;
   protected String username;
   protected String password;
   protected String datasource;
   protected String connectionFactoryClass;

   public AbstractJdbcCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * The class name of a JDBC driver to use with the built-in connection pooling
    */
   public S driverClass(String driverClass) {
      this.driverClass = driverClass;
      return self();
   }

   /**
    * The class of JDBC driver to use with the built-in connection pooling
    */
   public S driverClass(Class<? extends Driver> klass) {
      this.driverClass = klass.getName();
      return self();
   }

   /**
    * The JDBC URL to use with the built-in connection pooling
    */
   public S connectionUrl(String connectionUrl) {
      this.connectionUrl = connectionUrl;
      return self();
   }

   /**
    * The username used to connect with the built-in connection pooling
    */
   public S username(String userName) {
      this.username = userName;
      return self();
   }

   /**
    * The password used to connect with the built-in connection pooling
    */
   public S password(String password) {
      this.password = password;
      return self();
   }

   /**
    * The JNDI name of a container-managed datasource
    */
   public S datasource(String datasource) {
      this.datasource = datasource;
      return self();
   }

   /**
    * The class name of a {@link ConnectionFactory} to use to handle connections to a database. If
    * unspecified, a suitable one will be chosen based on the other parametes (i.e.
    * {@link ManagedConnectionFactory} if a datasource is specified or
    * {@link PooledConnectionFactory} if a connectionUrl is specified)
    */
   public S connectionFactoryClass(String connectionFactoryClass) {
      this.connectionFactoryClass = connectionFactoryClass;
      return self();
   }

   /**
    * The class of a {@link ConnectionFactory} to use to handle connections to a database. If
    * unspecified, a suitable one will be chosen based on the other parametes (i.e.
    * {@link ManagedConnectionFactory} if a datasource is specified or
    * {@link PooledConnectionFactory} if a connectionUrl is specified)
    */
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
      if (connectionFactoryClass == null) {
         if (datasource != null)
            connectionFactoryClass = ManagedConnectionFactory.class.getName();
         else
            connectionFactoryClass = PooledConnectionFactory.class.getName();
      }
   }

   /*
    * TODO: we should really be using inheritance here, but because of a javac bug it won't let me
    * invoke super.read() from subclasses complaining that abstract methods cannot be invoked. Will
    * open a bug and add the ID here
    */
   protected S readInternal(AbstractJdbcCacheStoreConfiguration template) {
      this.connectionFactoryClass(template.connectionFactoryClass());
      this.connectionUrl(template.connectionUrl());
      this.datasource(template.datasource());
      this.driverClass(template.driverClass());
      this.password(template.password());
      this.username(template.userName());

      // LockSupportStore-specific configuration
      lockAcquistionTimeout = template.lockAcquistionTimeout();
      lockConcurrencyLevel = template.lockConcurrencyLevel();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());

      return self();
   }
}
