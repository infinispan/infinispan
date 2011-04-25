package org.jboss.seam.infinispan.event.cache;

import javax.enterprise.event.Event;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvictedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

@Listener
public class CacheEntryEvictedAdapter extends
      AbstractAdapter<CacheEntryEvictedEvent> {

   public static final CacheEntryEvictedEvent EMTPTY = new CacheEntryEvictedEvent() {

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

   public CacheEntryEvictedAdapter(Event<CacheEntryEvictedEvent> event) {
      super(event);
   }

   @CacheEntryEvicted
   public void fire(CacheEntryEvictedEvent payload) {
      super.fire(payload);
   }

}
