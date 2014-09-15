package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;
import java.util.Map;

/**
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted}.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted
 */
@Listener
public class CacheEntriesEvictedAdapter<K, V> extends AbstractAdapter<CacheEntriesEvictedEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDICacheEntriesEvictedEvent implements CacheEntriesEvictedEvent<K, V> {
      private CacheEntriesEvictedEvent<K, V> decoratedEvent;

      private CDICacheEntriesEvictedEvent(CacheEntriesEvictedEvent<K, V> decoratedEvent) {
         this.decoratedEvent = decoratedEvent;
      }

      @Override
      public Map<K, V> getEntries() {
         return decoratedEvent.getEntries();
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
   public static final CacheEntriesEvictedEvent<?, ?> EMPTY = new CacheEntriesEvictedEvent<Object, Object>() {
      @Override
      public Map<Object, Object> getEntries() {
         return null;
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
   public static final TypeLiteral<CacheEntriesEvictedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<CacheEntriesEvictedEvent<?, ?>>() {
   };

   public CacheEntriesEvictedAdapter(Event<CacheEntriesEvictedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @CacheEntriesEvicted
   public void fire(CacheEntriesEvictedEvent<K, V> payload) {
      super.fire(new CDICacheEntriesEvictedEvent(payload));
   }
}
