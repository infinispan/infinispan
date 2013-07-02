package org.infinispan.cdi.event.cachemanager;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;

import javax.enterprise.event.Event;

/**
 * @author Pete Muir
 */
@Listener
public class CacheStartedAdapter extends AbstractAdapter<CacheStartedEvent> {

   public static final CacheStartedEvent EMPTY = new CacheStartedEvent() {

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

   public CacheStartedAdapter(Event<CacheStartedEvent> event, String cacheName) {
      super(event);
      this.cacheName = cacheName;
   }

   @Override
   @CacheStarted
   public void fire(CacheStartedEvent payload) {
      if (payload.getCacheName().equals(cacheName)) {
         super.fire(payload);
      }
   }
}
