package org.infinispan.configuration.cache;

import java.util.LinkedList;
import java.util.List;

public class LoadersConfigurationBuilder extends AbstractConfigurationChildBuilder<LoadersConfiguration> {

   private boolean passivation;
   private boolean preload;
   private boolean shared;
   private List<LoaderConfigurationBuilder> cacheLoaders;
   

   protected LoadersConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public LoadersConfigurationBuilder passivation(boolean b) {
      this.passivation = b;
      return this;
   }

   public LoadersConfigurationBuilder preload(boolean b) {
      this.preload = b;
      return this;
   }

   public LoadersConfigurationBuilder shared(boolean b) {
      this.shared = b;
      return this;
   }
   
   public LoaderConfigurationBuilder addCacheLoader() {
      LoaderConfigurationBuilder builder = new LoaderConfigurationBuilder(this);
      this.cacheLoaders.add(builder);
      return builder;
   }
   
   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   LoadersConfiguration create() {
      List<LoaderConfiguration> loaders = new LinkedList<LoaderConfiguration>();
      for (LoaderConfigurationBuilder loader : cacheLoaders)
         loaders.add(loader.create());
      return new LoadersConfiguration(passivation, preload, shared, loaders);
   }
   
}
