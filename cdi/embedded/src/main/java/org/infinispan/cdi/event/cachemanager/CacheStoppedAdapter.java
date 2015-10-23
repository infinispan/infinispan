package org.infinispan.cdi.event.cachemanager;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;

import javax.enterprise.event.Event;

/**
 * @author Pete Muir
 */
@Listener
public class CacheStoppedAdapter extends AbstractAdapter<CacheStoppedEvent> {

   public static final CacheStoppedEvent EMPTY = new CacheStoppedEvent() {

      @Override
      public Type getType() {
         return null;
      }

      @Override
      public EmbeddedCacheManager getCacheManager() {
         return null;
      }

      @Override
      public String getCacheName() {
         return null;
      }
   };

   private final String cacheName;

   public CacheStoppedAdapter(Event<CacheStoppedEvent> event, String cacheName) {
      super(event);
      this.cacheName = cacheName;
   }

   @Override
   @CacheStopped
   public void fire(CacheStoppedEvent payload) {
      if (payload.getCacheName().equals(cacheName)) {
         super.fire(payload);
      }
   }
}
