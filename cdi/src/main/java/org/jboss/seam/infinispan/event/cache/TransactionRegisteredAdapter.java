package org.jboss.seam.infinispan.event.cache;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

@Listener
public class TransactionRegisteredAdapter<K,V> extends
      AbstractAdapter<TransactionRegisteredEvent<K,V>> {

   public static final TransactionRegisteredEvent<?, ?>  EMPTY = new TransactionRegisteredEvent<Object, Object>() {

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

   };

   @SuppressWarnings("serial")
   public static final TypeLiteral<TransactionRegisteredEvent<?,?>> WILDCARD_TYPE = new TypeLiteral<TransactionRegisteredEvent<?,?>>() {};

   public TransactionRegisteredAdapter(Event<TransactionRegisteredEvent<K,V>> event) {
      super(event);
   }

   @TransactionRegistered
   public void fire(TransactionRegisteredEvent<K,V> payload) {
      super.fire(payload);
   }

}
