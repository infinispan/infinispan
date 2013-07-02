package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

/**
 * @author Pete Muir
 */
@Listener
public class TransactionRegisteredAdapter<K, V> extends AbstractAdapter<TransactionRegisteredEvent<K, V>> {

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

   @SuppressWarnings("serial")
   public static final TypeLiteral<TransactionRegisteredEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<TransactionRegisteredEvent<?, ?>>() {
   };

   public TransactionRegisteredAdapter(Event<TransactionRegisteredEvent<K, V>> event) {
      super(event);
   }

   @Override
   @TransactionRegistered
   public void fire(TransactionRegisteredEvent<K, V> payload) {
      super.fire(payload);
   }
}
