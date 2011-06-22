package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class CacheEntryLoadedAdapter<K,V> extends
      AbstractAdapter<CacheEntryLoadedEvent<K,V>> {

   public static final CacheEntryLoadedEvent<?,?> EMPTY = new CacheEntryLoadedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryLoadedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryLoadedEvent<?,?>>() {};

   public CacheEntryLoadedAdapter(Event<CacheEntryLoadedEvent<K,V>> event) {
      super(event);
   }

   @CacheEntryLoaded
   public void fire(CacheEntryLoadedEvent<K,V> payload) {
      super.fire(payload);
   }

}
