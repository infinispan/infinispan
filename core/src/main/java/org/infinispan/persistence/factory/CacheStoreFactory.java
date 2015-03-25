package org.infinispan.persistence.factory;

import org.infinispan.configuration.cache.StoreConfiguration;

/**
 * Creates Cache Store instances.
 *
 * <i>Needs to be implemented when loading Cache Stores from custom locations (e.g. custom location on the disk).</i>
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public interface CacheStoreFactory {

   /**
    * Returns new instance based on {@link org.infinispan.configuration.cache.StoreConfiguration}.
    *
    * @param storeConfiguration Configuration to be processed.
    * @return Instance configured by the {@link org.infinispan.configuration.cache.StoreConfiguration}.
    */
   <T> T createInstance(StoreConfiguration storeConfiguration);

   StoreConfiguration processConfiguration(StoreConfiguration storeConfiguration);
}
