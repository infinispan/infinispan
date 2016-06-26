package org.infinispan.server.core;

import java.util.Set;

/**
 * Defines an interface to be used when a cache is to be ignored by a server implementation.  Any implementation should
 * be thread safe and allow for concurrent methods to be invoked.
 * @author gustavonalle
 * @author wburns
 * @since 9.0
 */
public interface CacheIgnoreAware {

   /**
    * Replaces all ignored caches with the set provided
    * @param cacheNames the set of caches to now ignore
    */
   void setIgnoredCaches(Set<String> cacheNames);

   /**
    * No longer ignore the given cache if it was before
    * @param cacheName the cache to now not ignore
    */
   void unignore(String cacheName);

   /**
    * Ignores a given cache if it wasn't before
    * @param cacheName the cache to ignore
    */
   void ignoreCache(String cacheName);

   /**
    * Queries whether the cache is ignored
    * @param cacheName the cache to see if it is ignored
    * @return whether or not the cache is ignored
    */
   boolean isCacheIgnored(String cacheName);
}
