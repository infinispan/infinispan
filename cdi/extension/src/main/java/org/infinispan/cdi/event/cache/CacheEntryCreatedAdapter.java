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
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated}.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated
 */
@Listener
public class CacheEntryCreatedAdapter<K, V> extends AbstractAdapter<CacheEntryCreatedEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDICacheEntryCreatedEvent implements CacheEntryCreatedEvent<K, V> {
      private CacheEntryCreatedEvent<K, V> decoratedEvent;

      private CDICacheEntryCreatedEvent(CacheEntryCreatedEvent<K, V> decoratedEvent) {
         this.decoratedEvent = decoratedEvent;
      }

      @Override
      public V getValue() {
         return decoratedEvent.getValue();
      }

      @Override
      public boolean isCommandRetried() {
         return decoratedEvent.isCommandRetried();
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
      public Cache getCache() {
         return decoratedEvent.getCache();
      }
   }

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

   /**
    * Events which will be selected (including generic type information (<code><?, ?></code>).
    */
   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryCreatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryCreatedEvent<?, ?>>() {
   };

   /**
    * Needed for creating event bridge.
    */
   public CacheEntryCreatedAdapter(Event<CacheEntryCreatedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryCreated
   public void fire(CacheEntryCreatedEvent<K, V> payload) {
      super.fire(new CDICacheEntryCreatedEvent(payload));
   }
}
