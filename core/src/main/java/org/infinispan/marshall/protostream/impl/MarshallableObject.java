package org.infinispan.marshall.protostream.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A wrapper for interface implementations which can either be marshalled by the {@link
 * GlobalMarshaller} or the configured user marshaller if no internal marshaller exists for
 * the implementation. In such scenarios it's not possible to use {@link org.infinispan.protostream.WrappedMessage} as
 * it will ignore any configured user marshaller, and similarly, it's not possible to use {@link MarshallableUserObject}
 * as that exclusively utilises the user marshaller.
 * <p>
 * A good example of when this class is required, is for the marshalling of {@link Metadata} implementations. We utilise
 * many internal implementations, which always have a ProtoStream marshaller available, however it's also possible for
 * users to provide custom implementations which must be handled by the configured user marshaller.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_OBJECT)
public class MarshallableObject<T> extends AbstractMarshallableWrapper<T> {

   static final MarshallableObject<?> EMPTY_INSTANCE = new MarshallableObject<>((Object) null);

   /**
    * @param object the Object to be wrapped.
    * @return a new {@link MarshallableObject} instance containing the passed object if the object is not null,
    * otherwise null.
    */
   public static <T> MarshallableObject<T> create(T object) {
      return object == null ? null : new MarshallableObject<>(object);
   }

   /**
    * @param wrapper the {@link MarshallableObject} instance to unwrap.
    * @return the wrapped {@link Object} or null if the provided wrapper does not exist.
    */
   public static <T> T unwrap(MarshallableObject<T> wrapper) {
      return wrapper == null ? null : wrapper.get();
   }

   @ProtoFactory
   MarshallableObject(byte[] bytes) {
      super(bytes);
   }

   public MarshallableObject(T object) {
      super(object);
   }

   public static class Marshaller extends AbstractMarshallableWrapper.Marshaller {

      public Marshaller(String typeName, org.infinispan.commons.marshall.Marshaller userMarshaller) {
         super(typeName, userMarshaller);
      }

      @Override
      MarshallableObject newWrapperInstance(Object o) {
         return o == null ? EMPTY_INSTANCE : new MarshallableObject<>(o);
      }

      @Override
      public Class getJavaClass() {
         return MarshallableObject.class;
      }
   }
}
