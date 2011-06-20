package org.jboss.seam.infinispan.event.cache;

import javax.enterprise.event.Event;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

@Listener
public class CacheEntryInvalidatedAdapter extends
      AbstractAdapter<CacheEntryInvalidatedEvent> {

   public static final CacheEntryInvalidatedEvent EMTPTY = new CacheEntryInvalidatedEvent() {

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

   public CacheEntryInvalidatedAdapter(Event<CacheEntryInvalidatedEvent> event) {
      super(event);
   }

   @CacheEntryInvalidated
   public void fire(CacheEntryInvalidatedEvent payload) {
      super.fire(payload);
   }

}
