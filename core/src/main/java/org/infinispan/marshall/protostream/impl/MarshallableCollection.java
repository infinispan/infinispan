package org.infinispan.marshall.protostream.impl;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_COLLECTION)
public class MarshallableCollection<T> extends AbstractMarshallableCollection<T> {
   final Collection<T> collection;

   /**
    * @param collection the {@link Collection} to be wrapped.
    * @return a new {@link MarshallableCollection} instance containing the passed object if the object is not null,
    * otherwise null.
    */
   public static <T> MarshallableCollection<T> create(Collection<T> collection) {
      return collection == null ? null : new MarshallableCollection<>(collection);
   }

   /**
    * @param wrapper the {@link MarshallableCollection} instance to unwrap.
    * @return the wrapped {@link Collection} or null if the provided wrapper does not exist.
    */
   public static <T> Collection<T> unwrap(MarshallableCollection<T> wrapper) {
      return wrapper == null ? null : wrapper.get();
   }

   private MarshallableCollection(Collection<T> collection) {
      this.collection = collection;
   }

   @ProtoFactory
   MarshallableCollection(int size, Collection<T> bytes) {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   @ProtoField(2)
   Collection<byte[]> getBytes() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   public Collection<T> get() {
      return collection;
   }

   public static class Marshaller extends AbstractMarshallableCollection.Marshaller {

      public Marshaller(AbstractInternalProtoStreamMarshaller marshaller) {
         super(marshaller);
      }

      @Override
      Collection<Object> newCollection(int size) {
         return new ArrayList<>(size);
      }

      @Override
      AbstractMarshallableCollection<?> newWrapperInstance(Collection<?> collection) {
         return new MarshallableCollection<>(collection);
      }

      @Override
      public Class<MarshallableCollection> getJavaClass() {
         return MarshallableCollection.class;
      }
   }
}
