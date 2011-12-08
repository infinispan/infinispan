package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class LoadersConfigurationBuilder extends AbstractConfigurationChildBuilder<LoadersConfiguration> {

   private boolean passivation = false;
   private boolean preload = false;
   private boolean shared = false;
   private List<LoaderConfigurationBuilder> cacheLoaders = new ArrayList<LoaderConfigurationBuilder>();
   

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
   
   List<LoaderConfigurationBuilder> cacheLoaders() {
      return cacheLoaders;
   }
   
   @Override
   void validate() {
      for (LoaderConfigurationBuilder b : cacheLoaders) {
         b.validate();
      }
   }

   @Override
   LoadersConfiguration create() {
      List<LoaderConfiguration> loaders = new LinkedList<LoaderConfiguration>();
      for (LoaderConfigurationBuilder loader : cacheLoaders)
         loaders.add(loader.create());
      return new LoadersConfiguration(passivation, preload, shared, loaders);
   }
   
   @Override
   public LoadersConfigurationBuilder read(LoadersConfiguration template) {
      for (LoaderConfiguration c : template.cacheLoaders()) {
         // TODO is this right?!?
         this.addCacheLoader().read(c);
      }
      this.passivation = template.passivation();
      this.preload = template.preload();
      this.shared = template.shared();
      
      return this;
   }
   
}
