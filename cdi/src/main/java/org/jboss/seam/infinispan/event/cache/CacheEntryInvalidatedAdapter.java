package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class CacheEntryInvalidatedAdapter<K,V> extends
      AbstractAdapter<CacheEntryInvalidatedEvent<K,V>> {

   public static final CacheEntryInvalidatedEvent<?,?> EMPTY = new CacheEntryInvalidatedEvent<Object, Object>() {

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

      public Object getValue() {
         return null;
      }

   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryInvalidatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryInvalidatedEvent<?,?>>() {};

   public CacheEntryInvalidatedAdapter(Event<CacheEntryInvalidatedEvent<K,V>> event) {
      super(event);
   }

   @CacheEntryInvalidated
   public void fire(CacheEntryInvalidatedEvent<K,V> payload) {
      super.fire(payload);
   }

}
