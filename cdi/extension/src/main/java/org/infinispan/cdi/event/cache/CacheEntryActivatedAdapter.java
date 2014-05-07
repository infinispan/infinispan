package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryActivatedAdapter<K, V> extends AbstractAdapter<CacheEntryActivatedEvent<K, V>> {

   public static final CacheEntryActivatedEvent<?, ?> EMPTY = new CacheEntryActivatedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryActivatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryActivatedEvent<?, ?>>() {
   };

   public CacheEntryActivatedAdapter(Event<CacheEntryActivatedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryActivated
   public void fire(CacheEntryActivatedEvent<K, V> payload) {
      super.fire(payload);
   }
}
