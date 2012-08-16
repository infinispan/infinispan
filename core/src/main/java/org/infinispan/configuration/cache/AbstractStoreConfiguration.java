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

import org.infinispan.util.TypedProperties;

public abstract class AbstractStoreConfiguration extends AbstractLoaderConfiguration implements StoreConfiguration {

   private final boolean purgeOnStartup;
   private final boolean purgeSynchronously;
   private final int purgerThreads;

   private boolean fetchPersistentState;
   private boolean ignoreModifications;

   private final AsyncStoreConfiguration async;
   private final SingletonStoreConfiguration singletonStore;

   AbstractStoreConfiguration(boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState,
         boolean ignoreModifications, TypedProperties properties, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(properties);
      this.purgeOnStartup = purgeOnStartup;
      this.purgeSynchronously = purgeSynchronously;
      this.purgerThreads = purgerThreads;
      this.fetchPersistentState = fetchPersistentState;
      this.ignoreModifications = ignoreModifications;
      this.async = async;
      this.singletonStore = singletonStore;
   }

   /**
    * Configuration for the async cache loader. If enabled, this provides you with asynchronous
    * writes to the cache store, giving you 'write-behind' caching.
    */
   @Override
   public AsyncStoreConfiguration async() {
      return async;
   }

   /**
    * SingletonStore is a delegating cache store used for situations when only one instance in a
    * cluster should interact with the underlying store. The coordinator of the cluster will be
    * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real
    * CacheStore implementation. It always delegates reads to the real CacheStore.
    */
   @Override
   public SingletonStoreConfiguration singletonStore() {
      return singletonStore;
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   @Override
   public boolean purgeOnStartup() {
      return purgeOnStartup;
   }

   /**
    * If true, CacheStore#purgeExpired() call will be done synchronously
    */
   @Override
   public boolean purgeSynchronously() {
      return purgeSynchronously;
   }

   /**
    * The number of threads to use when purging asynchronously.
    */
   @Override
   public int purgerThreads() {
      return purgerThreads;
   }

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment, and only 'replication' and
    * 'invalidation' cluster modes are supported.
    */
   @Override
   public boolean fetchPersistentState() {
      return fetchPersistentState;
   }

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    */
   @Override
   public boolean ignoreModifications() {
      return ignoreModifications;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AbstractStoreConfiguration that = (AbstractStoreConfiguration) o;

      if (!super.equals(that)) return false;
      if (fetchPersistentState != that.fetchPersistentState) return false;
      if (ignoreModifications != that.ignoreModifications) return false;
      if (purgeOnStartup != that.purgeOnStartup) return false;
      if (purgeSynchronously != that.purgeSynchronously) return false;
      if (purgerThreads != that.purgerThreads) return false;
      if (async != null ? !async.equals(that.async) : that.async != null)
         return false;
      if (singletonStore != null ? !singletonStore.equals(that.singletonStore) : that.singletonStore != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = 31 * super.hashCode() + (purgeOnStartup ? 1 : 0);
      result = 31 * result + (purgeSynchronously ? 1 : 0);
      result = 31 * result + purgerThreads;
      result = 31 * result + (fetchPersistentState ? 1 : 0);
      result = 31 * result + (ignoreModifications ? 1 : 0);
      result = 31 * result + (async != null ? async.hashCode() : 0);
      result = 31 * result + (singletonStore != null ? singletonStore.hashCode() : 0);
      return result;
   }

}
