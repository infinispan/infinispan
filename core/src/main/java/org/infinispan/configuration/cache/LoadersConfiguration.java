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

import java.util.List;

/**
 * Configuration for cache loaders and stores.
 * 
 */
public class LoadersConfiguration {

   private final boolean passivation;
   private final boolean preload;
   private final boolean shared;
   private final List<AbstractLoaderConfiguration> cacheLoaders;

   LoadersConfiguration(boolean passivation, boolean preload, boolean shared, List<AbstractLoaderConfiguration> cacheLoaders) {
      this.passivation = passivation;
      this.preload = preload;
      this.shared = shared;
      this.cacheLoaders = cacheLoaders;
   }

   /**
    * If true, data is only written to the cache store when it is evicted from memory, a phenomenon
    * known as 'passivation'. Next time the data is requested, it will be 'activated' which means
    * that data will be brought back to memory and removed from the persistent store. This gives you
    * the ability to 'overflow' to disk, similar to swapping in an operating system. <br />
    * <br />
    * If false, the cache store contains a copy of the contents in memory, so writes to cache result
    * in cache store writes. This essentially gives you a 'write-through' configuration.
    */
   public boolean passivation() {
      return passivation;
   }

   /**
    * If true, when the cache starts, data stored in the cache store will be pre-loaded into memory.
    * This is particularly useful when data in the cache store will be needed immediately after
    * startup and you want to avoid cache operations being delayed as a result of loading this data
    * lazily. Can be used to provide a 'warm-cache' on startup, however there is a performance
    * penalty as startup time is affected by this process.
    */
   public boolean preload() {
      return preload;
   }

   /**
    * This setting should be set to true when multiple cache instances share the same cache store
    * (e.g., multiple nodes in a cluster using a JDBC-based CacheStore pointing to the same, shared
    * database.) Setting this to true avoids multiple cache instances writing the same modification
    * multiple times. If enabled, only the node where the modification originated will write to the
    * cache store.
    * <p/>
    * If disabled, each individual cache reacts to a potential remote update by storing the data to
    * the cache store. Note that this could be useful if each individual node has its own cache
    * store - perhaps local on-disk.
    */
   public boolean shared() {
      return shared;
   }

   public List<AbstractLoaderConfiguration> cacheLoaders() {
      return cacheLoaders;
   }

   /**
    * Loops through all individual cache loader configs and checks if fetchPersistentState is set on
    * any of them
    */
   public Boolean fetchPersistentState() {
      for (AbstractLoaderConfiguration c : cacheLoaders) {
         if (c.fetchPersistentState())
            return true;
      }
      return false;
   }

   public boolean usingCacheLoaders() {
      return !cacheLoaders.isEmpty();
   }

   public boolean usingAsyncStore() {
      for (AbstractLoaderConfiguration loaderConfig : cacheLoaders) {
         if (loaderConfig.async().enabled())
            return true;
      }
      return false;
   }

   public boolean usingChainingCacheLoader() {
      return !passivation() && cacheLoaders.size() > 1;
   }

   @Override
   public String toString() {
      return "LoadersConfiguration{" +
            "cacheLoaders=" + cacheLoaders +
            ", passivation=" + passivation +
            ", preload=" + preload +
            ", shared=" + shared +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LoadersConfiguration that = (LoadersConfiguration) o;

      if (passivation != that.passivation) return false;
      if (preload != that.preload) return false;
      if (shared != that.shared) return false;
      if (cacheLoaders != null ? !cacheLoaders.equals(that.cacheLoaders) : that.cacheLoaders != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (passivation ? 1 : 0);
      result = 31 * result + (preload ? 1 : 0);
      result = 31 * result + (shared ? 1 : 0);
      result = 31 * result + (cacheLoaders != null ? cacheLoaders.hashCode() : 0);
      return result;
   }

}
