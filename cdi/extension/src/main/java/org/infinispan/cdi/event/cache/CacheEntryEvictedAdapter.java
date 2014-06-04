package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvictedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class CacheEntryEvictedAdapter<K, V> extends AbstractAdapter<CacheEntryEvictedEvent<K, V>> {

   public static final CacheEntryEvictedEvent<?, ?> EMPTY = new CacheEntryEvictedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryEvictedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryEvictedEvent<?, ?>>() {
   };

   public CacheEntryEvictedAdapter(Event<CacheEntryEvictedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryEvicted
   public void fire(CacheEntryEvictedEvent<K, V> payload) {
      super.fire(payload);
   }
}
