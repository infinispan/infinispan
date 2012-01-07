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

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.util.TypedProperties;

public abstract class AbstractLoaderConfiguration extends AbstractTypedPropertiesConfiguration {

   private final boolean purgeOnStartup;
   private final boolean purgeSynchronously;
   private boolean fetchPersistentState;
   private boolean ignoreModifications;
   
   private final AsyncLoaderConfiguration async;
   private final SingletonStoreConfiguration singletonStore;
   
   AbstractLoaderConfiguration(boolean purgeOnStartup, boolean purgeSynchronously, boolean fetchPersistentState,
         boolean ignoreModifications, TypedProperties properties, AsyncLoaderConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(properties);
      this.purgeOnStartup = purgeOnStartup;
      this.purgeSynchronously = purgeSynchronously;
      this.fetchPersistentState = fetchPersistentState;
      this.ignoreModifications = ignoreModifications;
      this.async = async;
      this.singletonStore = singletonStore;
   }

   public AsyncLoaderConfiguration async() {
      return async;
   }
   
   public SingletonStoreConfiguration singletonStore() {
      return singletonStore;
   }

   public boolean purgeOnStartup() {
      return purgeOnStartup;
   }

   public boolean purgeSynchronously() {
      return purgeSynchronously;
   }

   public boolean fetchPersistentState() {
      return fetchPersistentState;
   }

   public boolean ignoreModifications() {
      return ignoreModifications;
   }

}
