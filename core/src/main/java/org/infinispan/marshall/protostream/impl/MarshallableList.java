package org.infinispan.marshall.protostream.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A wrapper for a {@link List} of objects whose type is unknown until runtime. This is equivalent to utilising a
 * <code>List<MarshallableObject></code> without the overhead of creating a {@link MarshallableObject} per
 * entry.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_LIST)
public class MarshallableList<T> extends AbstractMarshallableCollection<T> {

   /**
    * @param list the {@link List} to be wrapped.
    * @return a new {@link MarshallableList} instance containing the passed object if the object is not null,
    * otherwise null.
    */
   public static <T> MarshallableList<T> create(List<T> list) {
      return list == null ? null : new MarshallableList<>(list);
   }

   /**
    * @param wrapper the {@link MarshallableList} instance to unwrap.
    * @return the wrapped {@link List} or null if the provided wrapper does not exist.
    */
   public static <T> List<T> unwrap(MarshallableList<T> wrapper) {
      return wrapper == null ? null : wrapper.get();
   }

   final List<T> list;

   private MarshallableList(List<T> list) {
      this.list = list;
   }

   @ProtoFactory
   MarshallableList(int size, List<byte[]> bytes) {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   @ProtoField(2)
   List<byte[]> getBytes() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   public List<T> get() {
      return list;
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
         return new MarshallableList<>((List<?>) collection);
      }

      @Override
      public Class<MarshallableList> getJavaClass() {
         return MarshallableList.class;
      }
   }
}
