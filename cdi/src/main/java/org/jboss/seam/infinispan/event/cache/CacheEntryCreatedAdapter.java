package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;

@Listener
public class CacheEntryCreatedAdapter extends
      AbstractAdapter<CacheEntryCreatedEvent> {

   public static final CacheEntryCreatedEvent EMPTY = new CacheEntryCreatedEvent() {

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
   };

   public CacheEntryCreatedAdapter(Event<CacheEntryCreatedEvent> event) {
      super(event);
   }

   @CacheEntryCreated
   public void fire(CacheEntryCreatedEvent payload) {
      super.fire(payload);
   }

}
