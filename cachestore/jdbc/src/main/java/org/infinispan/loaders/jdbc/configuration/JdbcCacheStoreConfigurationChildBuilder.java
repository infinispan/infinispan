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

import org.infinispan.configuration.cache.LockSupportStoreConfigurationChildBuilder;

/**
 * JdbcCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface JdbcCacheStoreConfigurationChildBuilder<S extends AbstractJdbcCacheStoreConfigurationBuilder<?, S>> extends LockSupportStoreConfigurationChildBuilder<S> {

   /**
    * Configures a connection pool to be used by this JDBC Cache Store to handle connections to the database
    */
   PooledConnectionFactoryConfigurationBuilder<S> connectionPool();

   /**
    * Configures a DataSource to be used by this JDBC Cache Store to handle connections to the database
    */
   ManagedConnectionFactoryConfigurationBuilder<S> dataSource();


   /**
    * Configures this JDBC Cache Store to use a single connection to the database
    */
   SimpleConnectionFactoryConfigurationBuilder<S> simpleConnection();

   /**
    * Use the specified {@link ConnectionFactory} to handle connections to the database
    */
   //<C extends ConnectionFactoryConfigurationBuilder<?>> C connectionFactory(Class<C> klass);

}
