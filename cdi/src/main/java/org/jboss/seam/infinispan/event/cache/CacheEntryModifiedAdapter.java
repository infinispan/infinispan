package org.jboss.seam.infinispan.event.cache;

import javax.enterprise.event.Event;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

@Listener
public class CacheEntryModifiedAdapter extends
      AbstractAdapter<CacheEntryModifiedEvent> {

   public static final CacheEntryModifiedEvent EMTPTY = new CacheEntryModifiedEvent() {

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

   public CacheEntryModifiedAdapter(Event<CacheEntryModifiedEvent> event) {
      super(event);
   }

   @CacheEntryModified
   public void fire(CacheEntryModifiedEvent payload) {
      super.fire(payload);
   }

}
