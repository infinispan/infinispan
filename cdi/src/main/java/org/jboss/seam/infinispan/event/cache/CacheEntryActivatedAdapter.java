package org.jboss.seam.infinispan.event.cache;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

@Listener
public class CacheEntryActivatedAdapter<K, V> extends
      AbstractAdapter<CacheEntryActivatedEvent<K,V>> {

   public static final CacheEntryActivatedEvent<?, ?> EMPTY = new CacheEntryActivatedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryActivatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryActivatedEvent<?,?>>() {}; 

   public CacheEntryActivatedAdapter(Event<CacheEntryActivatedEvent<K, V>> event) {
      super(event);
   }

   @CacheEntryActivated
   public void fire(CacheEntryActivatedEvent<K, V> payload) {
      super.fire(payload);
   }

}
