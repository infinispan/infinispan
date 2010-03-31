package org.infinispan.manager;

import org.infinispan.Cache;

/**
 * Acts as an container of {@link org.infinispan.Cache}s. Each cache in the container is identified by a name.
 * It also contains a <b>default</b> cache.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface CacheContainer {
   /**
    * Retrieves the default cache associated with this cache container.
    * <p/>
    * As such, this method is always guaranteed to return the default cache.
    * <p />
    * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
    * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
    * that others may be using the cache.
    *
    * @return the default cache.
    */
   <K, V> Cache<K, V> getCache();

   /**
    * Retrieves a named cache from the system.  If the cache has been previously created with the same name, the running
    * cache instance is returned.  Otherwise, this method attempts to create the cache first.
    * <p/>
    * In the case of a {@link CacheManager}: when creating a new cache, this method will use the configuration passed in to the CacheManager on construction,
    * as a template, and then optionally apply any overrides previously defined for the named cache using the {@link
    * CacheManager#defineConfiguration(String, org.infinispan.config.Configuration)} or {@link CacheManager#defineConfiguration(String, String, org.infinispan.config.Configuration)}
    * methods, or declared in the configuration file.
    * <p />
    * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
    * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
    * that others may be using the cache.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   <K, V> Cache<K, V> getCache(String cacheName);
}
