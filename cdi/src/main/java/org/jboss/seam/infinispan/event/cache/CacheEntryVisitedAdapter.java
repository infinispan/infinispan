package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class CacheEntryVisitedAdapter<K,V> extends
      AbstractAdapter<CacheEntryVisitedEvent<K,V>> {

   public static final CacheEntryVisitedEvent<?,?> EMPTY = new CacheEntryVisitedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryVisitedEvent<?,?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryVisitedEvent<?,?>>() {};

   public CacheEntryVisitedAdapter(Event<CacheEntryVisitedEvent<K,V>> event) {
      super(event);
   }

   @CacheEntryVisited
   public void fire(CacheEntryVisitedEvent<K,V> payload) {
      super.fire(payload);
   }

}
