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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.Builder;
import org.infinispan.configuration.ConfigurationUtils;

/**
 * Configuration for cache loaders and stores.
 *
 */
public class LoadersConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<LoadersConfiguration> {

   private boolean passivation = false;
   private boolean preload = false;
   private boolean shared = false;
   private List<CacheLoaderConfigurationBuilder<?,?>> cacheLoaders = new ArrayList<CacheLoaderConfigurationBuilder<?,?>>(2);

   protected LoadersConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public LoadersConfigurationBuilder passivation(boolean b) {
      this.passivation = b;
      return this;
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
   boolean passivation() {
      return passivation;
   }

   /**
    * If true, when the cache starts, data stored in the cache store will be pre-loaded into memory.
    * This is particularly useful when data in the cache store will be needed immediately after
    * startup and you want to avoid cache operations being delayed as a result of loading this data
    * lazily. Can be used to provide a 'warm-cache' on startup, however there is a performance
    * penalty as startup time is affected by this process.
    */
   public LoadersConfigurationBuilder preload(boolean b) {
      this.preload = b;
      return this;
   }

   boolean preload() {
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
   public LoadersConfigurationBuilder shared(boolean b) {
      this.shared = b;
      return this;
   }

   boolean shared() {
      return shared;
   }

   @Deprecated
   public LoaderConfigurationBuilder addCacheLoader() {
      LoaderConfigurationBuilder builder = new LoaderConfigurationBuilder(this);
      this.cacheLoaders.add(builder);
      return builder;
   }

   /**
    * Adds a cache loader to the configuration. If possible use the alternate {@link #addLoader(CacheLoaderConfigurationBuilder)} and
    * {@link #addLoader(Class)} which will return appropriately typed builders
    *
    * @return
    */
   public LegacyLoaderConfigurationBuilder addLoader() {
      LegacyLoaderConfigurationBuilder builder = new LegacyLoaderConfigurationBuilder(this);
      this.cacheLoaders.add(builder);
      return builder;
   }

   /**
    * Adds a cache loader which uses the specified builder class to build its configuration
    *
    * @param klass
    * @return
    */
   public <T extends CacheLoaderConfigurationBuilder<?, ?>> T addLoader(Class<T> klass) {
      try {
         Constructor<T> constructor = klass.getDeclaredConstructor(LoadersConfigurationBuilder.class);
         T builder = constructor.newInstance(this);
         this.cacheLoaders.add(builder);
         return builder;
      } catch (Exception e) {
         throw new ConfigurationException("Could not instantiate loader configuration builder '" + klass.getName()
               + "'", e);
      }
   }

   /**
    * Adds a cache loader which uses the specified builder instance to build its configuration
    *
    * @param builder an instance of {@link CacheLoaderConfigurationBuilder}
    * @return
    */
   public CacheLoaderConfigurationBuilder<?, ?> addLoader(CacheLoaderConfigurationBuilder<?, ?> builder) {
      this.cacheLoaders.add(builder);
      return builder;
   }

   /**
    * Adds a cache store to the configuration. If possible use the alternate {@link #addStore(CacheStoreConfigurationBuilder)} and
    * {@link #addStore(Class)} which will return appropriately typed builders
    *
    * @return
    */
   public LegacyStoreConfigurationBuilder addStore() {
     LegacyStoreConfigurationBuilder builder = new LegacyStoreConfigurationBuilder(this);
     this.cacheLoaders.add(builder);
     return builder;
   }

   /**
    * Adds a cache store which uses the specified builder class to build its configuration
    *
    * @param klass
    * @return
    */
   public <T extends CacheStoreConfigurationBuilder<?, ?>> T addStore(Class<T> klass) {
      try {
         Constructor<T> constructor = klass.getDeclaredConstructor(LoadersConfigurationBuilder.class);
         T builder = constructor.newInstance(this);
         this.cacheLoaders.add(builder);
         return builder;
      } catch (Exception e) {
         throw new ConfigurationException("Could not instantiate store configuration builder '" + klass.getName()
               + "'", e);
      }
   }

   /**
    * Adds a cache store which uses the specified builder instance to build its configuration
    *
    * @param builder an instance of {@link CacheStoreConfigurationBuilder}
    */
   public CacheLoaderConfigurationBuilder<?, ?> addStore(CacheStoreConfigurationBuilder<?, ?> builder) {
      this.cacheLoaders.add(builder);
      return builder;
   }

   /**
    * Adds a cluster cache loader
    */
   public ClusterCacheLoaderConfigurationBuilder addClusterCacheLoader() {
      ClusterCacheLoaderConfigurationBuilder builder = new ClusterCacheLoaderConfigurationBuilder(this);
      this.cacheLoaders.add(builder);
      return builder;
   }

   /**
    * Adds a file cache store
    */
   public FileCacheStoreConfigurationBuilder addFileCacheStore() {
      FileCacheStoreConfigurationBuilder builder = new FileCacheStoreConfigurationBuilder(this);
      this.cacheLoaders.add(builder);
      return builder;
   }

   /**
    * Removes any configured cache loaders and stores from this builder
    */
   public LoadersConfigurationBuilder clearCacheLoaders() {
      this.cacheLoaders.clear();
      return this;
   }

   /**
    * Returns a list of the cache loader/store builders added to this builder
    *
    * @return
    */
   List<CacheLoaderConfigurationBuilder<?, ?>> cacheLoaders() {
      return cacheLoaders;
   }

   @Override
   public void validate() {
      for (CacheLoaderConfigurationBuilder<?, ?> b : cacheLoaders) {
         b.validate();
      }
   }

   @Override
   public LoadersConfiguration create() {
      List<CacheLoaderConfiguration> loaders = new LinkedList<CacheLoaderConfiguration>();
      for (CacheLoaderConfigurationBuilder<?, ?> loader : cacheLoaders)
         loaders.add(loader.create());
      return new LoadersConfiguration(passivation, preload, shared, loaders);
   }

   @SuppressWarnings("unchecked")
   @Override
   public LoadersConfigurationBuilder read(LoadersConfiguration template) {
      clearCacheLoaders();
      for (CacheLoaderConfiguration c : template.cacheLoaders()) {
         Class<? extends CacheLoaderConfigurationBuilder<?, ?>> builderClass = (Class<? extends CacheLoaderConfigurationBuilder<?, ?>>) ConfigurationUtils.builderFor(c);
         Builder<Object> builder = (Builder<Object>) this.addLoader(builderClass);
         builder.read(c);
      }
      this.passivation = template.passivation();
      this.preload = template.preload();
      this.shared = template.shared();

      return this;
   }

   @Override
   public String toString() {
      return "LoadersConfigurationBuilder{" +
            "cacheLoaders=" + cacheLoaders +
            ", passivation=" + passivation +
            ", preload=" + preload +
            ", shared=" + shared +
            '}';
   }

}
