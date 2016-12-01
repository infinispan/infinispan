package org.infinispan.cdi.embedded.event.cache;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;

import org.infinispan.Cache;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * @author Pete Muir
 */
@Listener
public class TopologyChangedAdapter<K, V> extends AbstractAdapter<TopologyChangedEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDITopologyChangedEvent implements TopologyChangedEvent<K, V> {
      private TopologyChangedEvent<K, V> decoratedEvent;

      private CDITopologyChangedEvent(TopologyChangedEvent<K, V> decoratedEvent) {
         this.decoratedEvent = decoratedEvent;
      }

      @Override
      public ConsistentHash getReadConsistentHashAtStart() {
         return decoratedEvent.getReadConsistentHashAtStart();
      }

      @Override
      public ConsistentHash getWriteConsistentHashAtStart() {
         return decoratedEvent.getWriteConsistentHashAtStart();
      }

      @Override
      public ConsistentHash getReadConsistentHashAtEnd() {
         return decoratedEvent.getReadConsistentHashAtEnd();
      }

      @Override
      public ConsistentHash getWriteConsistentHashAtEnd() {
         return decoratedEvent.getWriteConsistentHashAtEnd();
      }

      @Override
      public int getNewTopologyId() {
         return decoratedEvent.getNewTopologyId();
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
   public static final TypeLiteral<TopologyChangedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<TopologyChangedEvent<?, ?>>() {
   };

   public TopologyChangedAdapter(Event<TopologyChangedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @TopologyChanged
   public void fire(TopologyChangedEvent<K, V> payload) {
      super.fire(new CDITopologyChangedEvent(payload));
   }
}
