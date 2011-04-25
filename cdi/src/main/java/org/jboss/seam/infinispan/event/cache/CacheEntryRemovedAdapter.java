package org.jboss.seam.infinispan.event.cache;

import javax.enterprise.event.Event;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

@Listener
public class CacheEntryRemovedAdapter extends
      AbstractAdapter<CacheEntryRemovedEvent> {

   public static final CacheEntryRemovedEvent EMTPTY = new CacheEntryRemovedEvent() {

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

   public CacheEntryRemovedAdapter(Event<CacheEntryRemovedEvent> event) {
      super(event);
   }

   @CacheEntryRemoved
   public void fire(CacheEntryRemovedEvent payload) {
      super.fire(payload);
   }

}
