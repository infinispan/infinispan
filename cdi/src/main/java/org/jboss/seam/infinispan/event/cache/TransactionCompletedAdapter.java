package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;

@Listener
public class TransactionCompletedAdapter extends
      AbstractAdapter<TransactionCompletedEvent> {

   public static final TransactionCompletedEvent EMTPTY = new TransactionCompletedEvent() {

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

      public Cache<?, ?> getCache() {
         return null;
      }

      public boolean isTransactionSuccessful() {
         return false;
      }

   };

   public TransactionCompletedAdapter(Event<TransactionCompletedEvent> event) {
      super(event);
   }

   @TransactionCompleted
   public void fire(TransactionCompletedEvent payload) {
      super.fire(payload);
   }

}
