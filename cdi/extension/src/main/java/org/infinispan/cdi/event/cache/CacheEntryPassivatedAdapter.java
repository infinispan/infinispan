package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryPassivatedAdapter<K, V> extends AbstractAdapter<CacheEntryPassivatedEvent<K, V>> {

   public static final CacheEntryPassivatedEvent<?, ?> EMPTY = new CacheEntryPassivatedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryPassivatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryPassivatedEvent<?, ?>>() {
   };

   public CacheEntryPassivatedAdapter(Event<CacheEntryPassivatedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryPassivated
   public void fire(CacheEntryPassivatedEvent<K, V> payload) {
      super.fire(payload);
   }
}
