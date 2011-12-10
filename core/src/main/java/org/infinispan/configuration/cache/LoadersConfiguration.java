package org.infinispan.configuration.cache;

import java.util.List;

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

   public boolean passivation() {
      return passivation;
   }

   public boolean preload() {
      return preload;
   }

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
   
}
