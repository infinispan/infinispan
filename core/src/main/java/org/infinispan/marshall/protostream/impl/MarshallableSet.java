package org.infinispan.marshall.protostream.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A wrapper for a {@link Set} of objects whose type is unknown until runtime. This is equivalent to utilising a
 * <code>Set<MarshallableObject></code> without the overhead of creating a {@link MarshallableObject} per
 * entry.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_SET)
public class MarshallableSet<T> extends AbstractMarshallableCollection<T> {

   /**
    * @param set the {@link Set} to be wrapped.
    * @return a new {@link MarshallableSet} instance containing the passed object if the object is not null,
    * otherwise null.
    */
   public static <T> MarshallableSet<T> create(Set<T> set) {
      return set == null ? null : new MarshallableSet<>(set);
   }

   /**
    * @param wrapper the {@link MarshallableSet} instance to unwrap.
    * @return the wrapped {@link Set} or null if the provided wrapper does not exist.
    */
   public static <T> Set<T> unwrap(MarshallableSet<T> wrapper) {
      return wrapper == null ? null : wrapper.get();
   }

   public static <R extends Set<T>, T> R unwrap(MarshallableSet<T> wrapper, Function<Set<T>, R> builder) {
      if (wrapper == null)
         return null;

      return builder.apply(wrapper.get());
   }

   final Set<T> set;

   private MarshallableSet(Set<T> set) {
      this.set = set;
   }

   @ProtoFactory
   MarshallableSet(int size, Set<byte[]> bytes) {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   @ProtoField(2)
   Set<byte[]> getBytes() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   public Set<T> get() {
      return set;
   }

   public static class Marshaller extends AbstractMarshallableCollection.Marshaller {

      public Marshaller(AbstractInternalProtoStreamMarshaller marshaller) {
         super(marshaller);
      }

      @Override
      Collection<Object> newCollection(int size) {
         return new HashSet<>(size);
      }

      @Override
      AbstractMarshallableCollection<?> newWrapperInstance(Collection<?> collection) {
         return new MarshallableSet<>((Set<?>) collection);
      }

      @Override
      public Class<MarshallableSet> getJavaClass() {
         return MarshallableSet.class;
      }
   }
}
