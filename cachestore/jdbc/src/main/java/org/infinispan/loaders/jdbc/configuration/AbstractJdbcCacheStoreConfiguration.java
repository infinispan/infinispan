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

import org.infinispan.configuration.cache.AbstractLockSupportCacheStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.util.TypedProperties;

public abstract class AbstractJdbcCacheStoreConfiguration extends AbstractLockSupportCacheStoreConfiguration {

   private final String driverClass;
   private final String connectionUrl;
   private final String userName;
   private final String password;
   private final String connectionFactoryClass;
   private final String datasource;

   AbstractJdbcCacheStoreConfiguration(String driverClass, String connectionUrl, String userName, String password,
         String connectionFactoryClass, String datasource, long lockAcquistionTimeout,
         int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
         boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads,
            fetchPersistentState, ignoreModifications, properties, async, singletonStore);
      this.driverClass = driverClass;
      this.connectionUrl = connectionUrl;
      this.userName = userName;
      this.password = password;
      this.connectionFactoryClass = connectionFactoryClass;
      this.datasource = datasource;
   }

   public String driverClass() {
      return driverClass;
   }

   public String connectionUrl() {
      return connectionUrl;
   }

   public String userName() {
      return userName;
   }

   public String password() {
      return password;
   }

   public String connectionFactoryClass() {
      return connectionFactoryClass;
   }

   public String datasource() {
      return datasource;
   }
}
