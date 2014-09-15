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
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted}.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted
 */
@Listener
public class CacheEntryEvictedAdapter<K, V> extends AbstractAdapter<CacheEntryEvictedEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDICacheEntriesEvictedEvent implements CacheEntryEvictedEvent<K, V> {
      private CacheEntryEvictedEvent<K, V> decoratedEvent;

      private CDICacheEntriesEvictedEvent(CacheEntryEvictedEvent<K, V> decoratedEvent) {
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
   public static final CacheEntryEvictedEvent<?, ?> EMPTY = new CacheEntryEvictedEvent<Object, Object>() {

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
   public static final TypeLiteral<CacheEntryEvictedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntryEvictedEvent<?, ?>>() {
   };

   public CacheEntryEvictedAdapter(Event<CacheEntryEvictedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntryEvicted
   public void fire(CacheEntryEvictedEvent<K, V> payload) {
      super.fire(new CDICacheEntriesEvictedEvent(payload));
   }
}
