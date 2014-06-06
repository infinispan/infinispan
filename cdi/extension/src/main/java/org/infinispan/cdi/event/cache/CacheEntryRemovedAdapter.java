package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryRemovedAdapter<K, V> extends AbstractAdapter<CacheEntryRemovedEvent<K, V>> {

   public static final CacheEntryRemovedEvent<?, ?> EMPTY = new CacheEntryRemovedEvent<Object, Object>() {

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
      public Object getOldValue() {
         return null;
      }

      @Override
      public boolean isCommandRetried() {
         return false;
      }

   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryRemovedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryRemovedEvent<?, ?>>() {
   };

   public CacheEntryRemovedAdapter(Event<CacheEntryRemovedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryRemoved
   public void fire(CacheEntryRemovedEvent<K, V> payload) {
      super.fire(payload);
   }
}
