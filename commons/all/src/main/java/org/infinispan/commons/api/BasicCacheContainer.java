package org.infinispan.commons.api;

import java.util.Set;

/**
 * <code>BasicCacheContainer</code> defines the methods used to obtain a {@link org.infinispan.commons.api.BasicCache}.
  *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface BasicCacheContainer extends Lifecycle {
   /**
    * Retrieves the default cache associated with this cache container.
        *
    * @return the default cache.
    * @throws org.infinispan.commons.CacheConfigurationException if a default cache does not exist.
    */
   <K, V> BasicCache<K, V> getCache();

   /**
    * Retrieves a cache by name.
        * If the cache has been previously created with the same name, the running
    * cache instance is returned.
    * Otherwise, this method attempts to create the cache first.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   <K, V> BasicCache<K, V> getCache(String cacheName);

   /**
    * Forcefully stops a cache with the given name.
    *
    * <p>
    * This method will force a stop of the cache, even if it is not completely initialized.
    * </p>
    *
    * @param cacheName name of the cache to stop
    */
   void stopCache(String cacheName);

   /**
    * This method returns a collection of all cache names.
        * The configurations may have been defined via XML, in the programmatic configuration,
    * or at runtime.
        * Internal-only caches are not included.
    *
    * @return an immutable set of cache names registered in this cache manager.
    */
   Set<String> getCacheNames();
}
