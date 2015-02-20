package org.infinispan.persistence.factory;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Registry for multiple {@link CacheStoreFactory} objects.
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public class CacheStoreFactoryRegistry {

   private static final Log log = LogFactory.getLog(CacheStoreFactoryRegistry.class);

   private List<CacheStoreFactory> factories = new ArrayList<>();

   public CacheStoreFactoryRegistry() {
      factories.add(new LocalClassLoaderCacheStoreFactory());
   }

   @Inject
   public void inject(DeployedCacheStoreFactory deployedCacheStoreFactory) {
      addCacheStoreFactory(deployedCacheStoreFactory);
   }

   /**
    * Creates new Object based on configuration.
    *
    * @param storeConfiguration Cache store configuration.
    * @return Instance created based on the configuration.
    * @throws org.infinispan.commons.CacheConfigurationException when the instance couldn't be created.
    */
   public Object createInstance(StoreConfiguration storeConfiguration) {
      for(CacheStoreFactory factory : factories) {
         Object instance = factory.createInstance(storeConfiguration);
         if(instance != null) {
            return instance;
         }
      }
      throw log.unableToInstantiateClass(storeConfiguration);
   }

   /**
    * Adds a new factory for processing.
    *
    * @param cacheStoreFactory Factory to be added.
    */
   public void addCacheStoreFactory(CacheStoreFactory cacheStoreFactory) {
      factories.add(0, cacheStoreFactory);
   }

   /**
    * Removes all factories associated to this registry.
    */
   public void clearFactories() {
      factories.clear();
   }
}
