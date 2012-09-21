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
package org.infinispan.loaders.jdbm.configuration;

import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jdbm.JdbmCacheStoreConfig;
import org.infinispan.util.TypedProperties;

@BuiltBy(JdbmCacheStoreConfigurationBuilder.class)
public class JdbmCacheStoreConfiguration extends AbstractStoreConfiguration implements
      LegacyLoaderAdapter<JdbmCacheStoreConfig> {
   private final String comparatorClassName;
   private final int expiryQueueSize;
   private final String location;

   public JdbmCacheStoreConfiguration(String comparatorClassName, int expiryQueueSize, String location, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
         boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration asyncStoreConfiguration, SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            asyncStoreConfiguration, singletonStoreConfiguration);
      this.comparatorClassName = comparatorClassName;
      this.expiryQueueSize = expiryQueueSize;
      this.location = location;
   }

   public String comparatorClassName() {
      return comparatorClassName;
   }

   public int expiryQueueSize() {
      return expiryQueueSize;
   }

   public String location() {
      return location;
   }

   @Override
   public JdbmCacheStoreConfig adapt() {
      JdbmCacheStoreConfig config = new JdbmCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.setComparatorClassName(comparatorClassName);
      config.setExpiryQueueSize(expiryQueueSize);
      config.setLocation(location);

      return config;
   }

}
