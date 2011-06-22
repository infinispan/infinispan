package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class CacheEntryRemovedAdapter<K,V> extends
      AbstractAdapter<CacheEntryRemovedEvent<K,V>> {

   public static final CacheEntryRemovedEvent<?,?> EMPTY = new CacheEntryRemovedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryRemovedEvent<?,?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryRemovedEvent<?,?>>() {};

   public CacheEntryRemovedAdapter(Event<CacheEntryRemovedEvent<K,V>> event) {
      super(event);
   }

   @CacheEntryRemoved
   public void fire(CacheEntryRemovedEvent<K,V> payload) {
      super.fire(payload);
   }

}
