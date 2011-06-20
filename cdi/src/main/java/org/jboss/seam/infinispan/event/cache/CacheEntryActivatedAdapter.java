package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;

@Listener
public class CacheEntryActivatedAdapter extends
      AbstractAdapter<CacheEntryActivatedEvent> {

   public static final CacheEntryActivatedEvent EMPTY = new CacheEntryActivatedEvent() {

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

   public CacheEntryActivatedAdapter(Event<CacheEntryActivatedEvent> event) {
      super(event);
   }

   @CacheEntryActivated
   public void fire(CacheEntryActivatedEvent payload) {
      super.fire(payload);
   }

}
