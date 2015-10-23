package org.infinispan.cdi.event.cache;

import org.infinispan.Cache;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.transport.Address;

import javax.enterprise.event.Event;
import javax.enterprise.util.TypeLiteral;
import java.util.Collection;

/**
 * Event bridge for {@link org.infinispan.notifications.cachelistener.annotation.DataRehashed}.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.notifications.Listener
 * @see org.infinispan.notifications.cachelistener.annotation.DataRehashed
 */
@Listener
public class DataRehashedAdapter<K, V> extends AbstractAdapter<DataRehashedEvent<K, V>> {

   /**
    * CDI does not allow parametrized type for events (like <code><K,V></code>). This is why this wrapped needs to be
    * introduced. To ensure type safety, this needs to be linked to parent class (in other words this class can not
    * be static).
    */
   private class CDIDataRehashedEvent implements DataRehashedEvent<K, V> {
      private DataRehashedEvent<K, V> decoratedEvent;

      private CDIDataRehashedEvent(DataRehashedEvent<K, V> decoratedEvent) {
         this.decoratedEvent = decoratedEvent;
      }

      @Override
      public Collection<Address> getMembersAtStart() {
         return decoratedEvent.getMembersAtStart();
      }

      @Override
      public Collection<Address> getMembersAtEnd() {
         return decoratedEvent.getMembersAtEnd();
      }

      @Override
      public ConsistentHash getConsistentHashAtStart() {
         return decoratedEvent.getConsistentHashAtStart();
      }

      @Override
      public ConsistentHash getConsistentHashAtEnd() {
         return decoratedEvent.getConsistentHashAtEnd();
      }

      @Override
      public ConsistentHash getUnionConsistentHash() {
         return decoratedEvent.getUnionConsistentHash();
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

   public static final DataRehashedEvent<?, ?> EMPTY = new DataRehashedEvent<Object, Object>() {

      @Override
      public Collection<Address> getMembersAtStart() {
         return null;
      }

      @Override
      public Collection<Address> getMembersAtEnd() {
         return null;
      }

      @Override
      public ConsistentHash getConsistentHashAtStart() {
         return null;
      }

      @Override
      public ConsistentHash getConsistentHashAtEnd() {
         return null;
      }

      @Override
      public ConsistentHash getUnionConsistentHash() {
         return null;
      }

      @Override
      public int getNewTopologyId() {
         return 0;
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
   public static final TypeLiteral<DataRehashedEvent<?, ?>> WILDCARD_TYPE = new TypeLiteral<DataRehashedEvent<?, ?>>() {
   };

   public DataRehashedAdapter(Event<DataRehashedEvent<K, V>> event) {
      super(event);
   }

   @Override
   @DataRehashed
   public void fire(DataRehashedEvent<K, V> payload) {
      super.fire(new CDIDataRehashedEvent(payload));
   }
}
