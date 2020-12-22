package org.infinispan.notifications.cachemanagerlistener.event;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Common characteristics of events that occur on a cache manager
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Event {
   enum Type {
      CACHE_STARTED, CACHE_STOPPED, VIEW_CHANGED, MERGED, CONFIGURATION_CHANGED
   }

   EmbeddedCacheManager getCacheManager();

   Type getType();
}
