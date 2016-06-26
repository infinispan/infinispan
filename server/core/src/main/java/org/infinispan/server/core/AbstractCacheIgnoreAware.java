package org.infinispan.server.core;

import org.infinispan.commons.util.concurrent.ConcurrentHashSet;

import java.util.Set;

/**
 * Abstract class providing stock implementations for {@link CacheIgnoreAware} so all that is required is to extend
 * this class.
 * @author gustavonalle
 * @author wburns
 * @since 9.0
 */
public class AbstractCacheIgnoreAware implements CacheIgnoreAware {

   private Set<String> ignoredCaches = new ConcurrentHashSet<>();

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
