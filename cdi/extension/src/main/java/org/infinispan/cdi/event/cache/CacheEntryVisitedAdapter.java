package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryVisitedAdapter<K, V> extends AbstractAdapter<CacheEntryVisitedEvent<K, V>> {

   public static final CacheEntryVisitedEvent<?, ?> EMPTY = new CacheEntryVisitedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryVisitedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryVisitedEvent<?, ?>>() {
   };

   public CacheEntryVisitedAdapter(Event<CacheEntryVisitedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryVisited
   public void fire(CacheEntryVisitedEvent<K, V> payload) {
      super.fire(payload);
   }
}
