package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
   
   boolean passivation() {
      return passivation;
   }

   public LoadersConfigurationBuilder preload(boolean b) {
      this.preload = b;
      return this;
   }

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
