package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class TransactionCompletedAdapter<K,V> extends
      AbstractAdapter<TransactionCompletedEvent<K,V>> {

   public static final TransactionCompletedEvent<?, ?>  EMPTY = new TransactionCompletedEvent<Object, Object>() {

      public Type getType() {
         return null;
      }

      public GlobalTransaction getGlobalTransaction() {
         return null;
      }

      public boolean isOriginLocal() {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isPre() {
         return false;
      }

      public Cache<Object, Object> getCache() {
         return null;
      }

      public boolean isTransactionSuccessful() {
         return false;
      }

   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<TransactionCompletedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<TransactionCompletedEvent<?,?>>() {};

   public TransactionCompletedAdapter(Event<TransactionCompletedEvent<K,V>> event) {
      super(event);
   }

   @TransactionCompleted
   public void fire(TransactionCompletedEvent<K,V> payload) {
      super.fire(payload);
   }

}
