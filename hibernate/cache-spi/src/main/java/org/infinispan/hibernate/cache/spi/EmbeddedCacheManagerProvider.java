package org.infinispan.hibernate.cache.spi;

import java.util.Properties;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Supplies an {@link EmbeddedCacheManager} for use by Infinispan implementation
 * of the {@link org.hibernate.cache.spi.RegionFactory}.
 *
 * @author Paul Ferraro
 * @since 9.2
 */
public interface EmbeddedCacheManagerProvider {
   /**
    * Returns a {@link EmbeddedCacheManager} given the specified configuration properties.
    * @param properties configuration properties
    * @return a started cache manager.
    */
   EmbeddedCacheManager getEmbeddedCacheManager(Properties properties);
}
