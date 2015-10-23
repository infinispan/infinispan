package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.TransactionRegistered}.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.TransactionRegistered
 */
@Listener
public class TransactionRegisteredAdapter<K, V> extends AbstractAdapter<TransactionRegisteredEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDITransactionRegisteredEvent implements TransactionRegisteredEvent<K, V> {
      private TransactionRegisteredEvent<K, V> decoratedEvent;

      private CDITransactionRegisteredEvent(TransactionRegisteredEvent<K, V> decoratedEvent) {
         this.decoratedEvent = decoratedEvent;
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
   public static final TransactionRegisteredEvent<?, ?> EMPTY = new TransactionRegisteredEvent<Object, Object>() {

      @Override
      public Type getType() {
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

   };

   /**
    * Events which will be selected (including generic type information (<code><?, ?></code>).
    */
   @SuppressWarnings("serial")
   public static final TypeLiteral<TransactionRegisteredEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<TransactionRegisteredEvent<?, ?>>() {
   };

   public TransactionRegisteredAdapter(Event<TransactionRegisteredEvent<K, V>> event) {
      super(event);
   }

   @Override
   @TransactionRegistered
   public void fire(TransactionRegisteredEvent<K, V> payload) {
      super.fire(new CDITransactionRegisteredEvent(payload));
   }
}
