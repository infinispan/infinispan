package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryInvalidatedAdapter<K, V> extends AbstractAdapter<CacheEntryInvalidatedEvent<K, V>> {

   public static final CacheEntryInvalidatedEvent<?, ?> EMPTY = new CacheEntryInvalidatedEvent<Object, Object>() {

      @Override
      public Type getType() {
         return null;
      }

      @Override
      public Object getKey() {
         return null;
      }

      @Override
      public GlobalTransaction getGlobalTransaction() {
         return null;
      }

      @Override
      public boolean isOriginLocal() {
         return false;
      }

      @Override
      public boolean isPre() {
         return false;
      }

      @Override
      public Cache<Object, Object> getCache() {
         return null;
      }

      @Override
      public Object getValue() {
         return null;
      }

      @Override
      public Metadata getMetadata() {
         return null;
      }
   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryInvalidatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryInvalidatedEvent<?, ?>>() {
   };

   public CacheEntryInvalidatedAdapter(Event<CacheEntryInvalidatedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryInvalidated
   public void fire(CacheEntryInvalidatedEvent<K, V> payload) {
      super.fire(payload);
   }
}
