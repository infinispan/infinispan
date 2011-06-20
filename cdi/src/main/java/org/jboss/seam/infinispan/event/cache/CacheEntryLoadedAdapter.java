package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;

@Listener
public class CacheEntryLoadedAdapter extends
      AbstractAdapter<CacheEntryLoadedEvent> {

   public static final CacheEntryLoadedEvent EMTPTY = new CacheEntryLoadedEvent() {

      public Type getType() {
         return null;
      }

      public Object getKey() {
         return null;
      }

      public GlobalTransaction getGlobalTransaction() {
         return null;
      }

      public boolean isOriginLocal() {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isPre() {
         return false;
      }

      public Cache<?, ?> getCache() {
         return null;
      }

      public Object getValue() {
         return null;
      }
   };

   public CacheEntryLoadedAdapter(Event<CacheEntryLoadedEvent> event) {
      super(event);
   }

   @CacheEntryLoaded
   public void fire(CacheEntryLoadedEvent payload) {
      super.fire(payload);
   }

}
