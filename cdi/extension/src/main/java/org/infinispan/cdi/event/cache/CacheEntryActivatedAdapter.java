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
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated}.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated
 */
@Listener
public class CacheEntryActivatedAdapter<K, V> extends AbstractAdapter<CacheEntryActivatedEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDICacheEntryActivatedEvent implements CacheEntryActivatedEvent<K, V> {
      private CacheEntryActivatedEvent<K, V> decoratedEvent;

      private CDICacheEntryActivatedEvent(CacheEntryActivatedEvent<K, V> decoratedEvent) {
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

   /**
    * Events which will be selected (including generic type information (<code><?, ?></code>).
    */
   @SuppressWarnings("serial")
   public static final TypeLiteral<CacheEntryActivatedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryActivatedEvent<?, ?>>() {
   };

   public CacheEntryActivatedAdapter(Event<CacheEntryActivatedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryActivated
   public void fire(CacheEntryActivatedEvent<K, V> payload) {
      super.fire(new CDICacheEntryActivatedEvent(payload));
   }
}
