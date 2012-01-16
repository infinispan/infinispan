package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Configuration for cache loaders and stores.
 * 
 */
public class LoadersConfigurationBuilder extends AbstractConfigurationChildBuilder<LoadersConfiguration> {

   private boolean passivation = false;
   private boolean preload = false;
   private boolean shared = false;
   private List<AbstractLoaderConfigurationBuilder<?>> cacheLoaders = new ArrayList<AbstractLoaderConfigurationBuilder<?>>(2);

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

   public LoaderConfigurationBuilder addCacheLoader() {
      LoaderConfigurationBuilder builder = new LoaderConfigurationBuilder(this);
      this.cacheLoaders.add(builder);
      return builder;
   }

   public FileCacheStoreConfigurationBuilder addFileCacheStore() {
      FileCacheStoreConfigurationBuilder builder = new FileCacheStoreConfigurationBuilder(this);
      this.cacheLoaders.add(builder);
      return builder;
   }

   List<AbstractLoaderConfigurationBuilder<?>> cacheLoaders() {
      return cacheLoaders;
   }

   @Override
   void validate() {
      for (AbstractLoaderConfigurationBuilder<?> b : cacheLoaders) {
         b.validate();
      }
   }

   @Override
   LoadersConfiguration create() {
      List<AbstractLoaderConfiguration> loaders = new LinkedList<AbstractLoaderConfiguration>();
      for (AbstractLoaderConfigurationBuilder<?> loader : cacheLoaders)
         loaders.add(loader.create());
      return new LoadersConfiguration(passivation, preload, shared, loaders);
   }

   @Override
   public LoadersConfigurationBuilder read(LoadersConfiguration template) {
      for (AbstractLoaderConfiguration c : template.cacheLoaders()) {
         if (c instanceof LoaderConfiguration)
            this.addCacheLoader().read((LoaderConfiguration) c);
         else if (c instanceof FileCacheStoreConfiguration)
            this.addFileCacheStore().read((FileCacheStoreConfiguration) c);
      }
      this.passivation = template.passivation();
      this.preload = template.preload();
      this.shared = template.shared();

      return this;
   }

}
