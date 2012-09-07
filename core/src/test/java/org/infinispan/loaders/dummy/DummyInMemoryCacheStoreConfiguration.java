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
package org.infinispan.loaders.dummy;

import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore.Cfg;
import org.infinispan.util.TypedProperties;

@BuiltBy(DummyInMemoryCacheStoreConfigurationBuilder.class)
public class DummyInMemoryCacheStoreConfiguration extends AbstractStoreConfiguration implements
      LegacyLoaderAdapter<DummyInMemoryCacheStore.Cfg> {

   private final boolean debug;
   private final String storeName;
   private final Object failKey;

   protected DummyInMemoryCacheStoreConfiguration(boolean debug, String storeName, Object failKey,
         boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState,
         boolean ignoreModifications, TypedProperties properties, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            async, singletonStore);
      this.debug = debug;
      this.storeName = storeName;
      this.failKey = failKey;
   }

   public boolean debug() {
      return debug;
   }

   public String storeName() {
      return storeName;
   }

   public Object failKey() {
      return failKey;
   }

   @Override
   public Cfg adapt() {
      Cfg config = new Cfg();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.debug(debug);
      config.storeName(storeName);
      config.failKey(failKey);

      return config;
   }

}
