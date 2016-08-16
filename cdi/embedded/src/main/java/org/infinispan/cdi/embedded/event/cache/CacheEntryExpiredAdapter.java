package org.infinispan.cdi.embedded.event.cache;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

import org.infinispan.Cache;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired}.
 *
 * @author William Burns
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired
 */
@Listener
public class CacheEntryExpiredAdapter<K, V> extends AbstractAdapter<CacheEntryExpiredEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDICacheEntriesEvictedEvent implements CacheEntryExpiredEvent<K, V> {
      private CacheEntryExpiredEvent<K, V> decoratedEvent;

      private CDICacheEntriesEvictedEvent(CacheEntryExpiredEvent<K, V> decoratedEvent) {
         this.decoratedEvent = decoratedEvent;
      }

      @Override
      public V getValue() {
         return decoratedEvent.getValue();
      }

      @Override
      public K getKey() {
         return decoratedEvent.getKey();
      }

      @Override
      public Metadata getMetadata() {
         return decoratedEvent.getMetadata();
      }

      @Override
      public GlobalTransaction getGlobalTransaction() {
         return decoratedEvent.getGlobalTransaction();
      }

      @Override
      public boolean isOriginLocal() {
         return decoratedEvent.isOriginLocal();
      }

      @Override
      public Type getType() {
         return decoratedEvent.getType();
      }

      @Override
      public boolean isPre() {
         return decoratedEvent.isPre();
      }

      @Override
      public Cache<K, V> getCache() {
         return decoratedEvent.getCache();
      }
   }

   /**
    * Needed for creating event bridge.
    */
   public static final CacheEntryExpiredEvent<?, ?> EMPTY = new CacheEntryExpiredEvent<Object, Object>() {

      @Override
      public Object getKey() {
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
      public GlobalTransaction getGlobalTransaction() {
         return null;
      }

      @Override
      public boolean isOriginLocal() {
         return false;
      }

      @Override
      public Type getType() {
         return null;
      }

      @Override
      public boolean isPre() {
         return false;
      }

      @Override
      public Cache<Object, Object> getCache() {
         return null;
      }
   };

   /**
    * Events which will be selected (including generic type information (<code><?, ?></code>).
    */
   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryExpiredEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryExpiredEvent<?, ?>>() {
   };

   public CacheEntryExpiredAdapter(Event<CacheEntryExpiredEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryExpired
   public void fire(CacheEntryExpiredEvent<K, V> payload) {
      super.fire(new CDICacheEntriesEvictedEvent(payload));
   }
}
