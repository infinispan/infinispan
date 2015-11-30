package org.infinispan.server.core

import java.util

/**
 * @author gustavonalle
 * @since 8.1
 */
trait CacheIgnoreAware {

   private val ignoredCaches = new util.HashSet[String]()

   def setIgnoredCaches(cacheNames: java.util.Set[String]) = {
      this.synchronized {
         ignoredCaches.clear()
         ignoredCaches.addAll(cacheNames)
      }
   }

   def unignore(cacheName: String) = this.synchronized {
      ignoredCaches.remove(cacheName)
   }

   def ignoreCache(cacheName: String) = this.synchronized {
      ignoredCaches.add(cacheName)
   }

   def isCacheIgnored(cacheName: String) = this.synchronized {
      ignoredCaches.contains(cacheName)
   }

}
