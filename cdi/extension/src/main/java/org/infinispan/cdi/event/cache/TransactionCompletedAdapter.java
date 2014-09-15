package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.TransactionCompleted}.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.TransactionCompleted
 */
@Listener
public class TransactionCompletedAdapter<K, V> extends AbstractAdapter<TransactionCompletedEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDITransactionCompletedEvent implements TransactionCompletedEvent<K, V> {
      private TransactionCompletedEvent<K, V> decoratedEvent;

      private CDITransactionCompletedEvent(TransactionCompletedEvent<K, V> decoratedEvent) {
         this.decoratedEvent = decoratedEvent;
      }

      @Override
      public boolean isTransactionSuccessful() {
         return decoratedEvent.isTransactionSuccessful();
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
   public static final TransactionCompletedEvent<?, ?> EMPTY = new TransactionCompletedEvent<Object, Object>() {

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

      @Override
      public boolean isTransactionSuccessful() {
         return false;
      }

   };

   /**
    * Events which will be selected (including generic type information (<code><?, ?></code>).
    */
   @SuppressWarnings("serial")
   public static final TypeLiteral<TransactionCompletedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<TransactionCompletedEvent<?, ?>>() {
   };

   public TransactionCompletedAdapter(Event<TransactionCompletedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @TransactionCompleted
   public void fire(TransactionCompletedEvent<K, V> payload) {
      super.fire(new CDITransactionCompletedEvent(payload));
   }
}
