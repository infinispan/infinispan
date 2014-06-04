package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryCreatedAdapter<K, V> extends AbstractAdapter<CacheEntryCreatedEvent<K, V>> {

   public static final CacheEntryCreatedEvent<?, ?> EMPTY = new CacheEntryCreatedEvent<Object, Object>() {

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

      @Override
      public boolean isCommandRetried() {
         return false;
      }

   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryCreatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryCreatedEvent<?, ?>>() {
   };

   public CacheEntryCreatedAdapter(Event<CacheEntryCreatedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryCreated
   public void fire(CacheEntryCreatedEvent<K, V> payload) {
      super.fire(payload);
   }
}
