package org.jboss.as.clustering.infinispan.cs.factory;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.persistence.factory.CacheStoreFactory;
import org.jboss.as.clustering.infinispan.cs.configuration.DeployedStoreConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Cache Store factory designed for deployed instances.
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public class DeployedCacheStoreFactory implements CacheStoreFactory {

   private Map<String, DeployedCacheStoreMetadata> deployedCacheStores = Collections.synchronizedMap(new HashMap<String, DeployedCacheStoreMetadata>());

   @Override
   public <T> T createInstance(StoreConfiguration cfg) {
      if(cfg instanceof DeployedStoreConfiguration) {
         DeployedStoreConfiguration deployedConfiguration = (DeployedStoreConfiguration) cfg;
         DeployedCacheStoreMetadata deployedCacheStoreMetadata = deployedCacheStores.get(deployedConfiguration.getCustomStoreClassName());
         return (T) deployedCacheStoreMetadata.getLoaderWriterRawInstance();
      }
      return null;
   }

   @Override
   public StoreConfiguration processConfiguration(StoreConfiguration storeConfiguration) {
      if(storeConfiguration instanceof DeployedStoreConfiguration) {
         DeployedStoreConfiguration deployedConfiguration = (DeployedStoreConfiguration) storeConfiguration;
         PersistenceConfigurationBuilder replacedBuilder = deployedConfiguration.getPersistenceConfigurationBuilder();
         DeployedCacheStoreMetadata deployedCacheStoreMetadata = deployedCacheStores.get(deployedConfiguration.getCustomStoreClassName());

         if(deployedCacheStoreMetadata == null) {
            throw new IllegalStateException("Could not find Deployed Cache metadata for " + deployedConfiguration.getCustomStoreClassName());
         }

         StoreConfigurationBuilder replacedStoreBuilder = replacedBuilder.addStore(deployedCacheStoreMetadata.getStoreBuilderClass());
         replacedStoreBuilder.fetchPersistentState(deployedConfiguration.fetchPersistentState());
         replacedStoreBuilder.ignoreModifications(deployedConfiguration.ignoreModifications());
         replacedStoreBuilder.preload(deployedConfiguration.preload());
         replacedStoreBuilder.purgeOnStartup(deployedConfiguration.purgeOnStartup());
         replacedStoreBuilder.shared(deployedConfiguration.shared());
         replacedStoreBuilder.withProperties(deployedConfiguration.properties());
         StoreConfiguration replacedConfiguration = (StoreConfiguration) replacedStoreBuilder.create();

         return replacedConfiguration;
      }
      return null;
   }

   public void addInstance(Object instance) {
      DeployedCacheStoreMetadata deployedCacheStoreMetadata = DeployedCacheStoreMetadata.fromDeployedStoreInstance(instance);
      deployedCacheStores.put(deployedCacheStoreMetadata.getDeployedCacheClassName(), deployedCacheStoreMetadata);
   }

   public void removeInstance(Object instance) {
      deployedCacheStores.remove(instance.getClass().getName());
   }
}
