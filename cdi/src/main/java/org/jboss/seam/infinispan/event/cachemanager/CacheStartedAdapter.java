package org.jboss.seam.infinispan.event.cachemanager;

import javax.enterprise.event.Event;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;

@Listener
public class CacheStartedAdapter extends AbstractAdapter<CacheStartedEvent> {

   public static final CacheStartedEvent EMTPTY = new CacheStartedEvent() {

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

   public CacheStartedAdapter(Event<CacheStartedEvent> event, String cacheName) {
      super(event);
      this.cacheName = cacheName;
   }

   @CacheStarted
   public void fire(CacheStartedEvent payload) {
      if (payload.getCacheName().equals(cacheName)) {
         super.fire(payload);
      }
   }

}
