package org.infinispan.commons.api;

import java.util.Set;

/**
 * <tt>BasicCacheContainer</tt> defines the methods used to obtain a {@link org.infinispan.commons.api.BasicCache}.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface BasicCacheContainer extends Lifecycle {
   /**
    * Retrieves the default cache associated with this cache container.
    * <p/>
    *
    * @return the default cache.
    * @throws org.infinispan.commons.CacheConfigurationException if a default cache does not exist.
    */
   <K, V> BasicCache<K, V> getCache();

   /**
    * Retrieves a cache by name.
    * <p/>
    * If the cache has been previously created with the same name, the running
    * cache instance is returned.
    * Otherwise, this method attempts to create the cache first.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   <K, V> BasicCache<K, V> getCache(String cacheName);

   /**
    * This method returns a collection of all cache names.
    * <p/>
    * The configurations may have been defined via XML, in the programmatic configuration,
    * or at runtime.
    * <p/>
    * Internal-only caches are not included.
    *
    * @return an immutable set of cache names registered in this cache manager.
    */
   Set<String> getCacheNames();
}
