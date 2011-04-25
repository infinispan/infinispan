package org.jboss.seam.infinispan.event.cache;

import javax.enterprise.event.Event;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

@Listener
public class CacheEntryPassivatedAdapter extends
      AbstractAdapter<CacheEntryPassivatedEvent> {

   public static final CacheEntryPassivatedEvent EMTPTY = new CacheEntryPassivatedEvent() {

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
      
      @Override
      public Object getValue() {
         return null;
      }
   };

   public CacheEntryPassivatedAdapter(Event<CacheEntryPassivatedEvent> event) {
      super(event);
   }

   @CacheEntryPassivated
   public void fire(CacheEntryPassivatedEvent payload) {
      super.fire(payload);
   }

}
