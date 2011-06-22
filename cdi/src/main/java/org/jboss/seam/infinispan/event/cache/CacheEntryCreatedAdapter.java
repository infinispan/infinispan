package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class CacheEntryCreatedAdapter<K,V> extends
      AbstractAdapter<CacheEntryCreatedEvent<K,V>> {

   public static final CacheEntryCreatedEvent<?,?>  EMPTY = new CacheEntryCreatedEvent<Object, Object>() {

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
   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryCreatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryCreatedEvent<?,?>>() {};

   public CacheEntryCreatedAdapter(Event<CacheEntryCreatedEvent<K,V>> event) {
      super(event);
   }

   @CacheEntryCreated
   public void fire(CacheEntryCreatedEvent<K,V> payload) {
      super.fire(payload);
   }

}
