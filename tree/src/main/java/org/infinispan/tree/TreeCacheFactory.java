package org.infinispan.tree;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.tree.impl.TreeCacheImpl;

/**
 * Factory class that contains API for users to create instances of {@link org.infinispan.tree.TreeCache}
 *
 * @author Navin Surtani
 */
public class TreeCacheFactory {

   /**
    * Creates a TreeCache instance by taking in a {@link org.infinispan.Cache} as a parameter
    *
    * @param cache
    * @return instance of a {@link TreeCache}
    * @throws NullPointerException   if the cache parameter is null
    * @throws CacheConfigurationException if the invocation batching configuration is not enabled.
    */

   public <K, V> TreeCache<K, V> createTreeCache(Cache<K, V> cache) {

      // Validation to make sure that the cache is not null.

      if (cache == null) {
         throw new NullPointerException("The cache parameter passed in is null");
      }

      // If invocationBatching is not enabled, throw a new configuration exception.
      if (!cache.getCacheConfiguration().invocationBatching().enabled()) {
         throw new CacheConfigurationException("invocationBatching is not enabled for cache '" +
               cache.getName() + "'. Make sure this is enabled by" +
               " calling configurationBuilder.invocationBatching().enable()");
      }

      return new TreeCacheImpl<K, V>(cache);
   }
}
