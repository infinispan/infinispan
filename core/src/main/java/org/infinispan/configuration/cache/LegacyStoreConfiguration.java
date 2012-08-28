/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

import org.infinispan.configuration.BuiltBy;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.util.TypedProperties;

/**
 * Configuration a legacy cache store, i.e. one which doesn't provide its own configuration builder
 *
 * @author Pete Muir
 * @author Tristan Tarrant
 * @since 5.2
 *
 */
@BuiltBy(LegacyStoreConfigurationBuilder.class)
public class LegacyStoreConfiguration extends AbstractStoreConfiguration {

   private final CacheLoader cacheStore; // TODO: in 6.0, as we deprecate the cacheLoader() method in LegacyStoreConfigurationBuilder, narrow this type to CacheStore

   LegacyStoreConfiguration(TypedProperties properties, CacheLoader cacheStore, boolean fetchPersistentState,
         boolean ignoreModifications, boolean purgeOnStartup, int purgerThreads, boolean purgeSynchronously,
         AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            async, singletonStore);
      this.cacheStore = cacheStore;
   }

   public CacheLoader cacheStore() {
      return cacheStore;
   }

   @Override
   public String toString() {
      return "StoreConfiguration{" + "cacheStore=" + cacheStore + ", purgeOnStartup=" + purgeOnStartup()
            + ", purgeSynchronously=" + purgeSynchronously() + ", purgerThreads=" + purgerThreads()
            + ", fetchPersistentState=" + fetchPersistentState() + ", ignoreModifications=" + ignoreModifications()
            + ", properties=" + properties() + ", async=" + async() + ", singletonStore=" + singletonStore() + '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;
      if (!super.equals(o))
         return false;

      LegacyStoreConfiguration that = (LegacyStoreConfiguration) o;

      if (cacheStore != null ? !cacheStore.equals(that.cacheStore) : that.cacheStore != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (cacheStore != null ? cacheStore.hashCode() : 0);
      return result;
   }

}
