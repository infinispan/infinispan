package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;

/**
 * Cache manager operations which affect the whole cluster. An instance of this can be retrieved from
 * {@link EmbeddedCacheManager#administration()}
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface EmbeddedCacheManagerAdmin extends CacheContainerAdmin<EmbeddedCacheManagerAdmin> {

   /**
    * Creates a cache across the cluster. The cache will survive topology changes, e.g. when a new node joins the cluster,
    * it will automatically be created there. This method will wait for the cache to be created on all nodes before
    * returning.
    *
    * @param name the name of the cache
    * @param configuration the configuration to use. It must be a clustered configuration (e.g. distributed)
    * @param <K> the generic type of the key
    * @param <V> the generic type of the value
    * @return the cache
    */
   <K, V> Cache<K, V> createCache(String name, Configuration configuration);
}
