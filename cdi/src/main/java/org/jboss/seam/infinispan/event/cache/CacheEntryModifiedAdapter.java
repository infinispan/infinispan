package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class CacheEntryModifiedAdapter<K,V> extends
      AbstractAdapter<CacheEntryModifiedEvent<K,V>> {

   public static final CacheEntryModifiedEvent<?,?> EMPTY = new CacheEntryModifiedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryModifiedEvent<?,?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryModifiedEvent<?,?>>() {};

   public CacheEntryModifiedAdapter(Event<CacheEntryModifiedEvent<K,V>> event) {
      super(event);
   }

   @CacheEntryModified
   public void fire(CacheEntryModifiedEvent<K,V> payload) {
      super.fire(payload);
   }

}
