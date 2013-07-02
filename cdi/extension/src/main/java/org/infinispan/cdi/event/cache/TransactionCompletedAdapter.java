package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class TransactionCompletedAdapter<K, V> extends AbstractAdapter<TransactionCompletedEvent<K, V>> {

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

   @SuppressWarnings("serial")
   public static final TypeLiteral<TransactionCompletedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<TransactionCompletedEvent<?, ?>>() {
   };

   public TransactionCompletedAdapter(Event<TransactionCompletedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @TransactionCompleted
   public void fire(TransactionCompletedEvent<K, V> payload) {
      super.fire(payload);
   }
}
