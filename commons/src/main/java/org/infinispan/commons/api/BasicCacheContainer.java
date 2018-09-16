package org.infinispan.commons.api;

import java.util.Set;

/**
 * <tt>BasicCacheContainer</tt> defines the methods used to obtain a {@link org.infinispan.api.BasicCache}.
 *
 *
 * @see org.infinispan.manager.EmbeddedCacheManager
 * @see org.infinispan.client.hotrod.RemoteCacheManager
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface BasicCacheContainer extends Lifecycle {
   @Deprecated
   String DEFAULT_CACHE_NAME = "___defaultcache";

   /**
    * Retrieves the default cache associated with this cache container.
    * <p>
    * As such, this method is always guaranteed to return the default cache, unless one has not been supplied to the
    * cache container.
    * <p>
    * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
    * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
    * that others may be using the cache.
    *
    * @return the default cache.
    */
   <K, V> BasicCache<K, V> getCache();

   /**
    * Retrieves a named cache from the system.  If the cache has been previously created with the same name, the running
    * cache instance is returned.  Otherwise, this method attempts to create the cache first.
    * <p>
    * In the case of a {@link org.infinispan.manager.EmbeddedCacheManager}: when creating a new cache, this method will
    * use the configuration passed in to the EmbeddedCacheManager on construction, as a template, and then optionally
    * apply any overrides previously defined for the named cache using the {@link EmbeddedCacheManager#defineConfiguration(String, org.infinispan.config.Configuration)}
    * or {@link EmbeddedCacheManager#defineConfiguration(String, String, org.infinispan.config.Configuration)}
    * methods, or declared in the configuration file.
    * <p>
    * <b>NB:</b> Shared caches are supported (and in fact encouraged) but if they are used it's the users responsibility to
    * ensure that <i>at least one</i> but <i>only one</i> caller calls stop() on the cache, and it does so with the awareness
    * that others may be using the cache.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   <K, V> BasicCache<K, V> getCache(String cacheName);

   /**
    * This method returns a collection of caches names which contains the
    * caches that have been defined via XML or programmatically, and the
    * caches that have been created at runtime via this cache manager
    * instance.
    *
    * If no named caches are registered or no caches have been created, this
    * method returns an empty set.  The list of caches does not include
    * internal-only caches {@link org.infinispan.registry.InternalCacheRegistry}
    *
    * @return an immutable set of caches registered or
    * created with this cache manager.
    */
   Set<String> getCacheNames();
}
