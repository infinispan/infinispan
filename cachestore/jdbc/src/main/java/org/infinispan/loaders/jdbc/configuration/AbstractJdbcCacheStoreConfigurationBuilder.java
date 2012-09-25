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

import java.lang.reflect.Constructor;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.ConfigurationUtils;
import org.infinispan.configuration.cache.AbstractLockSupportStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;

public abstract class AbstractJdbcCacheStoreConfigurationBuilder<T extends AbstractJdbcCacheStoreConfiguration, S extends AbstractJdbcCacheStoreConfigurationBuilder<T, S>> extends
      AbstractLockSupportStoreConfigurationBuilder<T, S> implements JdbcCacheStoreConfigurationChildBuilder<S> {

   protected ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration> connectionFactory;

   public AbstractJdbcCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public PooledConnectionFactoryConfigurationBuilder<S> connectionPool() {
      return connectionFactory(PooledConnectionFactoryConfigurationBuilder.class);
   }

   @Override
   public ManagedConnectionFactoryConfigurationBuilder<S> dataSource() {
      return connectionFactory(ManagedConnectionFactoryConfigurationBuilder.class);
   }

   @Override
   public SimpleConnectionFactoryConfigurationBuilder<S> simpleConnection() {
      return connectionFactory(SimpleConnectionFactoryConfigurationBuilder.class);
   }

   /**
    * Use the specified {@link ConnectionFactory} to handle connection to the database
    */
   public <C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(Class<C> klass) {
      if (connectionFactory != null) {
         throw new IllegalStateException("A ConnectionFactory has already been configured for this store");
      }
      try {
         Constructor<C> constructor = klass.getDeclaredConstructor(AbstractJdbcCacheStoreConfigurationBuilder.class);
         C builder = constructor.newInstance(this);
         this.connectionFactory = (ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration>) builder;
         return builder;
      } catch (Exception e) {
         throw new ConfigurationException("Could not instantiate loader configuration builder '" + klass.getName() + "'", e);
      }
   }

   @Override
   public void validate() {
      super.validate();
      if (connectionFactory == null) {
         throw new ConfigurationException("A ConnectionFactory has not been specified for the Store");
      }
   }

   /*
    * TODO: we should really be using inheritance here, but because of a javac bug it won't let me
    * invoke super.read() from subclasses complaining that abstract methods cannot be invoked. Will
    * open a bug and add the ID here
    */
   protected S readInternal(AbstractJdbcCacheStoreConfiguration template) {
      Class<? extends ConnectionFactoryConfigurationBuilder<?>> cfb = (Class<? extends ConnectionFactoryConfigurationBuilder<?>>) ConfigurationUtils.builderFor(template.connectionFactory());
      connectionFactory(cfb);
      connectionFactory.read(template.connectionFactory());

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
