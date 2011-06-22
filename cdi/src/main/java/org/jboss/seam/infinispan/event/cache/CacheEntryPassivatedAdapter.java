package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class CacheEntryPassivatedAdapter<K,V> extends
      AbstractAdapter<CacheEntryPassivatedEvent<K,V>> {

   public static final CacheEntryPassivatedEvent<?, ?>  EMPTY = new CacheEntryPassivatedEvent<Object, Object>() {

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

      public Cache<Object, Object> getCache() {
         return null;
      }

      @Override
      public Object getValue() {
         return null;
      }
   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryPassivatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryPassivatedEvent<?,?>>() {};

   public CacheEntryPassivatedAdapter(Event<CacheEntryPassivatedEvent<K,V>> event) {
      super(event);
   }

   @CacheEntryPassivated
   public void fire(CacheEntryPassivatedEvent<K,V> payload) {
      super.fire(payload);
   }

}
