package org.infinispan.marshall.protostream.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A wrapper message used by ProtoStream Marshallers to allow user objects to be marshalled/unmarshalled via the {@link
 * MarshallableUserObject.Marshaller} implementation, which delegates to the configured user marshaller in {@link
 * org.infinispan.configuration.global.SerializationConfiguration} if it exists.
 * <p>
 * This abstraction hides the details of the configured user marshaller from our internal Pojos, so that all calls to
 * the configured user marshaller can be limited to the {@link MarshallableUserObject.Marshaller} instance.
 * <p>
 * In order to allow this object to be utilised by our internal ProtoStream annotated Pojos, we need to generate the
 * proto schema for this object using the protostream-processor and {@link org.infinispan.protostream.annotations.AutoProtoSchemaBuilder}.
 * Consequently, it's necessary for the generated marshaller to be overridden, therefore calls to {@link
 * org.infinispan.protostream.SerializationContext#registerMarshaller(BaseMarshaller)} must be made after the
 * registration of any generated {@link org.infinispan.protostream.SerializationContextInitializer}'s that contain this
 * class. To ensure that the marshaller generated for this class is never used, we throw a {@link IllegalStateException}
 * if the {@link ProtoFactory} constructor is called.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
// TODO avoid use in commands?
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_USER_OBJECT)
public class MarshallableUserObject<T> extends AbstractMarshallableWrapper<T> {

   static final MarshallableUserObject<?> EMPTY_INSTANCE = new MarshallableUserObject<>((Object) null);

   /**
    * @param wrapper the {@link MarshallableUserObject} instance to unwrap.
    * @return the wrapped {@link Object} or null if the provided wrapper does not exist.
    */
   public static <T> T unwrap(MarshallableUserObject<T> wrapper) {
      return wrapper == null ? null : wrapper.get();
   }

   /**
    * @param object the Object to be wrapped.
    * @return a new {@link MarshallableUserObject} instance containing the passed object if the object is not null,
    * otherwise null.
    */
   public static <T> MarshallableUserObject<T> create(T object) {
      return object == null ? null : new MarshallableUserObject<>(object);
   }

   @ProtoFactory
   MarshallableUserObject(byte[] bytes) {
      super(bytes);
   }

   public MarshallableUserObject(T object) {
      super(object);
   }

   public static class Marshaller extends AbstractMarshallableWrapper.Marshaller {

      public Marshaller(String typeName, org.infinispan.commons.marshall.Marshaller userMarshaller) {
         super(typeName, userMarshaller);
      }

      @Override
      MarshallableUserObject newWrapperInstance(Object o) {
         return o == null ? EMPTY_INSTANCE : new MarshallableUserObject<>(o);
      }

      @Override
      public Class getJavaClass() { return MarshallableUserObject.class; }
   }
}
