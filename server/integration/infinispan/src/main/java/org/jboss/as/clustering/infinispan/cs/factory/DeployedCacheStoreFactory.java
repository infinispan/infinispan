package org.jboss.as.clustering.infinispan.cs.factory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.persistence.factory.CacheStoreFactory;
import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.as.clustering.infinispan.cs.configuration.DeployedStoreConfiguration;

/**
 * Cache Store factory designed for deployed instances.
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public class DeployedCacheStoreFactory implements CacheStoreFactory {

   private static final int TIMEOUT_SECONDS = 60;

   private ConcurrentHashMap<String, CompletableFuture<DeployedCacheStoreMetadata>> deployedCacheStores = new ConcurrentHashMap<>();

   @Override
   public <T> T createInstance(StoreConfiguration cfg) {
      if (cfg instanceof DeployedStoreConfiguration) {
         DeployedStoreConfiguration deployedConfiguration = (DeployedStoreConfiguration) cfg;
         DeployedCacheStoreMetadata deployedCacheStoreMetadata;
         try {
            InfinispanLogger.ROOT_LOGGER.debug(String.format("Waiting for deployment of Custom Cache Store (%s).", deployedConfiguration.getCustomStoreClassName()));
            deployedCacheStoreMetadata = getPromise(deployedConfiguration.getCustomStoreClassName()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return (T) deployedCacheStoreMetadata.getLoaderWriterRawInstance();
         } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("An error occurred while processing the deployment", e);
         } catch (TimeoutException e) {
            InfinispanLogger.ROOT_LOGGER.loadingCustomCacheStoreTimeout(deployedConfiguration.getCustomStoreClassName());
         }
      }
      return null;
   }

   @Override
   public StoreConfiguration processConfiguration(StoreConfiguration storeConfiguration) {
      if (storeConfiguration instanceof DeployedStoreConfiguration) {
         DeployedStoreConfiguration deployedConfiguration = (DeployedStoreConfiguration) storeConfiguration;
         PersistenceConfigurationBuilder replacedBuilder = deployedConfiguration.getPersistenceConfigurationBuilder();

         try {
            DeployedCacheStoreMetadata deployedCacheStoreMetadata = getPromise(deployedConfiguration.getCustomStoreClassName()).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            StoreConfigurationBuilder replacedStoreBuilder = replacedBuilder.addStore(deployedCacheStoreMetadata.getStoreBuilderClass());
            replacedStoreBuilder.fetchPersistentState(deployedConfiguration.fetchPersistentState());
            replacedStoreBuilder.ignoreModifications(deployedConfiguration.ignoreModifications());
            replacedStoreBuilder.preload(deployedConfiguration.preload());
            replacedStoreBuilder.purgeOnStartup(deployedConfiguration.purgeOnStartup());
            replacedStoreBuilder.maxBatchSize(deployedConfiguration.maxBatchSize());
            replacedStoreBuilder.shared(deployedConfiguration.shared());
            replacedStoreBuilder.withProperties(deployedConfiguration.properties());

            return (StoreConfiguration) replacedStoreBuilder.create();
         } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("An error occurred while processing the deployment", e);
         } catch (TimeoutException e) {
            InfinispanLogger.ROOT_LOGGER.loadingCustomCacheStoreTimeout(deployedConfiguration.getCustomStoreClassName());
         }
      }
      return null;
   }

   public void addInstance(Object instance) {
      DeployedCacheStoreMetadata deployedCacheStoreMetadata = DeployedCacheStoreMetadata.fromDeployedStoreInstance(instance);
      getPromise(deployedCacheStoreMetadata.getDeployedCacheClassName()).complete(deployedCacheStoreMetadata);
   }

   public void removeInstance(Object instance) {
      deployedCacheStores.remove(instance.getClass().getName());
   }

   private CompletableFuture<DeployedCacheStoreMetadata> getPromise(String key) {
      return deployedCacheStores.computeIfAbsent(key, mappedKey -> new CompletableFuture<>());
   }
}
