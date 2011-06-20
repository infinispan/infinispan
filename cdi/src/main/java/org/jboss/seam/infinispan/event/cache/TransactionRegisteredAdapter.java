package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;

@Listener
public class TransactionRegisteredAdapter extends
      AbstractAdapter<TransactionRegisteredEvent> {

   public static final TransactionRegisteredEvent EMPTY = new TransactionRegisteredEvent() {

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

   };

   public TransactionRegisteredAdapter(Event<TransactionRegisteredEvent> event) {
      super(event);
   }

   @TransactionRegistered
   public void fire(TransactionRegisteredEvent payload) {
      super.fire(payload);
   }

}
