package org.infinispan.server.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract class providing stock implementations for {@link CacheIgnoreAware} so all that is required is to extend
 * this class.
 * @author gustavonalle
 * @author wburns
 * @since 9.0
 */
public class AbstractCacheIgnoreAware implements CacheIgnoreAware {

   private Set<String> ignoredCaches = ConcurrentHashMap.newKeySet();

   public void setIgnoredCaches(Set<String> cacheNames) {
      ignoredCaches.clear();
      cacheNames.forEach(ignoredCaches::add);
   }

   public void unignore(String cacheName) {
      ignoredCaches.remove(cacheName);
   }

   public void ignoreCache(String cacheName) {
      ignoredCaches.add(cacheName);
   }

   public boolean isCacheIgnored(String cacheName) {
      return ignoredCaches.contains(cacheName);
   }

}
