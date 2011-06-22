package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvictedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class CacheEntryEvictedAdapter<K,V> extends
      AbstractAdapter<CacheEntryEvictedEvent<K,V>> {

   public static final CacheEntryEvictedEvent<?,?> EMPTY = new CacheEntryEvictedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryEvictedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryEvictedEvent<?,?>>() {};

   public CacheEntryEvictedAdapter(Event<CacheEntryEvictedEvent<K,V>> event) {
      super(event);
   }

   @CacheEntryEvicted
   public void fire(CacheEntryEvictedEvent<K,V> payload) {
      super.fire(payload);
   }

}
