package org.jboss.seam.infinispan.event.cachemanager;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;

import javax.enterprise.event.Event;

@Listener
public class CacheStoppedAdapter extends AbstractAdapter<CacheStoppedEvent> {

   public static final CacheStoppedEvent EMTPTY = new CacheStoppedEvent() {

      public Type getType() {
         return null;
      }

      public EmbeddedCacheManager getCacheManager() {
         return null;
      }

      public String getCacheName() {
         return null;
      }
   };

   private final String cacheName;

   public CacheStoppedAdapter(Event<CacheStoppedEvent> event, String cacheName) {
      super(event);
      this.cacheName = cacheName;
   }

   @CacheStopped
   public void fire(CacheStoppedEvent payload) {
      if (payload.getCacheName().equals(cacheName)) {
         super.fire(payload);
      }
   }

}
