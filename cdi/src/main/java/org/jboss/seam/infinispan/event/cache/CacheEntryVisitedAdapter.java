package org.jboss.seam.infinispan.event.cache;

import javax.enterprise.event.Event;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

@Listener
public class CacheEntryVisitedAdapter extends
      AbstractAdapter<CacheEntryVisitedEvent> {

   public static final CacheEntryVisitedEvent EMTPTY = new CacheEntryVisitedEvent() {

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

   public CacheEntryVisitedAdapter(Event<CacheEntryVisitedEvent> event) {
      super(event);
   }

   @CacheEntryVisited
   public void fire(CacheEntryVisitedEvent payload) {
      super.fire(payload);
   }

}
