package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryLoadedAdapter<K, V> extends AbstractAdapter<CacheEntryLoadedEvent<K, V>> {

   public static final CacheEntryLoadedEvent<?, ?> EMPTY = new CacheEntryLoadedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryLoadedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryLoadedEvent<?, ?>>() {
   };

   public CacheEntryLoadedAdapter(Event<CacheEntryLoadedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryLoaded
   public void fire(CacheEntryLoadedEvent<K, V> payload) {
      super.fire(payload);
   }
}
