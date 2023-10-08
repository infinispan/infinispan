package org.infinispan.marshall.protostream.impl;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Queue;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A wrapper for a {@link Deque} of objects whose type is unknown until runtime. This is equivalent to utilising a
 * <code>Deque<MarshallableObject></code> without the overhead of creating a {@link MarshallableObject} per
 * entry.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_DEQUE)
public class MarshallableDeque<T> extends AbstractMarshallableCollection<T> {

   /**
    * @param queue the {@link Queue} to be wrapped as a {@link Deque}
    * @return a new {@link MarshallableDeque} instance containing the passed object if the object is not null,
    * otherwise null. If the passed queue is not a {@link Deque} implementation, a new one is created.
    */
   public static <T> MarshallableDeque<T> create(Queue<T> queue) {
      return queue == null ? null :
            queue instanceof Deque<T> deque ?
                  new MarshallableDeque<>(deque) :
                  new MarshallableDeque<>(new ArrayDeque<>(queue));
   }

   /**
    * @param deque the {@link Deque} to be wrapped.
    * @return a new {@link MarshallableDeque} instance containing the passed object if the object is not null,
    * otherwise null.
    */
   public static <T> MarshallableDeque<T> create(Deque<T> deque) {
      return deque == null ? null : new MarshallableDeque<>(deque);
   }

   /**
    * @param wrapper the {@link MarshallableDeque} instance to unwrap.
    * @return the wrapped {@link Deque} or null if the provided wrapper does not exist.
    */
   public static <T> Deque<T> unwrap(MarshallableDeque<T> wrapper) {
      return wrapper == null ? null : wrapper.get();
   }

   final Deque<T> deque;

   private MarshallableDeque(Deque<T> deque) {
      this.deque = deque;
   }

   @ProtoFactory
   MarshallableDeque(int size, Deque<byte[]> bytes) {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   @ProtoField(value = 2, collectionImplementation = ArrayDeque.class)
   Deque<byte[]> getBytes() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   public Deque<T> get() {
      return deque;
   }

   public static class Marshaller extends AbstractMarshallableCollection.Marshaller {

      public Marshaller(AbstractInternalProtoStreamMarshaller marshaller) {
         super(marshaller);
      }

      @Override
      Collection<Object> newCollection(int size) {
         return new ArrayDeque<>(size);
      }

      @Override
      AbstractMarshallableCollection<?> newWrapperInstance(Collection<?> collection) {
         return new MarshallableDeque<>((Deque<?>) collection);
      }

      @Override
      public Class<MarshallableDeque> getJavaClass() {
         return MarshallableDeque.class;
      }
   }
}
