package org.infinispan.server.core;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Defines an interface to be used when a cache is to be ignored by a server implementation.  Any implementation should
 * be thread safe and allow for concurrent methods to be invoked.
 * @author gustavonalle
 * @author wburns
 * @since 9.0
 */
public interface CacheIgnoreAware {

   /**
    * No longer ignore the given cache if it was before
    * @param cacheName the cache to now not ignore
    */
   CompletionStage<Boolean> unignore(String cacheName);

   /**
    * Ignores a given cache if it wasn't before
    * @param cacheName the cache to ignore
    */
   CompletionStage<Void> ignoreCache(String cacheName);

   /**
    * Queries whether the cache is ignored
    * @param cacheName the cache to see if it is ignored
    * @return whether or not the cache is ignored
    */
   boolean isCacheIgnored(String cacheName);

   /**
    * @return Set of all cache names currently ignored for the cache manager
    */
   CompletionStage<Collection<String>> getIgnoredCaches(String cacheManager);

   void initialize(EmbeddedCacheManager cacheManager);
}
