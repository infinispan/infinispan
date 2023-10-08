package org.infinispan.marshall.protostream.impl;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.WireType;

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

   public static class Marshaller implements ProtobufTagMarshaller<MarshallableUserObject> {

      private final String typeName;
      private final org.infinispan.commons.marshall.Marshaller userMarshaller;


      public Marshaller(String typeName, org.infinispan.commons.marshall.Marshaller userMarshaller) {
         this.typeName = typeName;
         this.userMarshaller = userMarshaller;
      }

      @Override
      public Class<MarshallableUserObject> getJavaClass() { return MarshallableUserObject.class; }

      @Override
      public String getTypeName() { return typeName; }

      @Override
      public MarshallableUserObject read(ReadContext ctx) throws IOException {
         TagReader in = ctx.getReader();
         try {
            byte[] bytes = null;
            boolean done = false;
            while (!done) {
               final int tag = in.readTag();
               switch (tag) {
                  // end of message
                  case 0:
                     done = true;
                     break;
                  // field number 1
                  case 1 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED: {
                     bytes = in.readByteArray();
                     break;
                  }
                  default: {
                     if (!in.skipField(tag)) done = true;
                  }
               }
            }
            Object userObject = userMarshaller.objectFromByteBuffer(bytes);
            return new MarshallableUserObject<>(userObject);
         } catch (ClassNotFoundException e) {
            throw new MarshallingException(e);
         }
      }

      @Override
      public void write(WriteContext ctx, MarshallableUserObject marshallableUserObject) throws IOException {
         try {
            Object userObject = marshallableUserObject.get();
            byte[] bytes = userMarshaller.objectToByteBuffer(userObject);
            // field number 1
            ctx.getWriter().writeBytes(1, bytes);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheException(e);
         }
      }
   }
}
