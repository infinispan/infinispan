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
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved}.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved
 */
@Listener
public class CacheEntryRemovedAdapter<K, V> extends AbstractAdapter<CacheEntryRemovedEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDICacheEntryRemovedEvent implements CacheEntryRemovedEvent<K, V> {
      private CacheEntryRemovedEvent<K, V> decoratedEvent;

      private CDICacheEntryRemovedEvent(CacheEntryRemovedEvent<K, V> decoratedEvent) {
         this.decoratedEvent = decoratedEvent;
      }

      @Override
      public V getValue() {
         return decoratedEvent.getValue();
      }

      @Override
      public V getOldValue() {
         return decoratedEvent.getOldValue();
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
      public Cache<K, V> getCache() {
         return decoratedEvent.getCache();
      }
   }

   /**
    * Needed for creating event bridge.
    */
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

   /**
    * Events which will be selected (including generic type information (<code><?, ?></code>).
    */
   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryRemovedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryRemovedEvent<?, ?>>() {
   };

   public CacheEntryRemovedAdapter(Event<CacheEntryRemovedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryRemoved
   public void fire(CacheEntryRemovedEvent<K, V> payload) {
      super.fire(new CDICacheEntryRemovedEvent(payload));
   }
}
