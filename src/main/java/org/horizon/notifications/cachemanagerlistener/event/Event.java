package org.horizon.notifications.cachemanagerlistener.event;

import org.horizon.manager.CacheManager;

/**
 * Common characteristics of events that occur on a cache manager
 *
 * @author Manik Surtani
 * @since 1.0
 */
public interface Event {
   public enum Type {
      CACHE_STARTED, CACHE_STOPPED, VIEW_CHANGED
   }

   CacheManager getCacheManager();

   Type getType();
}
