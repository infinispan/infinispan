package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryModifiedAdapter<K, V> extends AbstractAdapter<CacheEntryModifiedEvent<K, V>> {

   public static final CacheEntryModifiedEvent<?, ?> EMPTY = new CacheEntryModifiedEvent<Object, Object>() {

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
      public boolean isCreated() {
         return false;
      }

      @Override
      public boolean isCommandRetried() {
         return false;
      }

   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryModifiedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryModifiedEvent<?, ?>>() {
   };

   public CacheEntryModifiedAdapter(Event<CacheEntryModifiedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryModified
   public void fire(CacheEntryModifiedEvent<K, V> payload) {
      super.fire(payload);
   }
}
