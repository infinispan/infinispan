package org.infinispan.test.integration.as.cdi;


import org.infinispan.Cache;
import org.infinispan.eviction.EvictionStrategy;

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheRemoveAll;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;

/**
 * <p>The greeting cache manager.</p>
 *
 * <p>This manager is used to collect informations on the greeting cache and to clear it's content if needed.</p>
 *
 * @author Kevin Pollet <pollet.kevin@gmail.com> (C) 2011
 * @see javax.cache.annotation.CacheRemoveAll
 */
@Named
@ApplicationScoped
public class GreetingCacheManager {

   @Inject
   @GreetingCache
   private Cache<CacheKey, String> cache;

   public String getCacheName() {
      return cache.getName();
   }

   public int getNumberOfEntries() {
      return cache.size();
   }

   public EvictionStrategy getEvictionStrategy() {
      return cache.getCacheConfiguration().eviction().strategy();
   }

   public int getEvictionMaxEntries() {
      return cache.getCacheConfiguration().eviction().maxEntries();
   }

   public long getExpirationLifespan() {
      return cache.getCacheConfiguration().expiration().lifespan();
   }

   public String[] getCachedValues() {
      Collection<String> cachedValues = cache.values();
      return cachedValues.toArray(new String[cachedValues.size()]);
   }

   @CacheRemoveAll(cacheName = "greeting-cache")
   public void clearCache() {
   }

}
