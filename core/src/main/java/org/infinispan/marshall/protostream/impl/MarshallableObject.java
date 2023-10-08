package org.infinispan.marshall.protostream.impl;

import static org.infinispan.marshall.protostream.impl.GlobalContextInitializer.getFqTypeName;

import java.io.IOException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.annotations.impl.GeneratedMarshallerBase;
import org.infinispan.protostream.descriptors.WireType;

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

   static final MarshallableObject<?> EMPTY_INSTANCE = new MarshallableObject<>(null);

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
   MarshallableObject(byte[] bytes, WrappedMessage message) {
      super(bytes);
   }

   public MarshallableObject(T object) {
      super(object);
   }

   @ProtoField(2)
   WrappedMessage getMessage() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   public static class Marshaller extends GeneratedMarshallerBase implements ProtobufTagMarshaller<MarshallableObject> {

      private final String typeName;
      private final GlobalMarshaller marshaller;

      private volatile org.infinispan.protostream.impl.BaseMarshallerDelegate<WrappedMessage> delegate;

      public Marshaller(GlobalMarshaller marshaller) {
         this.typeName = getFqTypeName(MarshallableObject.class);
         this.marshaller = marshaller;
      }

      @Override
      public String getTypeName() {
         return typeName;
      }

      @Override
      public Class<MarshallableObject> getJavaClass() {
         return MarshallableObject.class;
      }

      @Override
      public MarshallableObject<?> read(ReadContext ctx) throws IOException {
         final TagReader in = ctx.getReader();
         WrappedMessage message = null;
         byte[] bytes = null;
         boolean done = false;
         while (!done) {
            final int tag = in.readTag();
            switch (tag) {
               case 0:
                  done = true;
                  break;
               case 1 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED: {
                  bytes = in.readByteArray();
                  break;
               }
               case (2 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED): {
                  if (delegate == null)
                     delegate = ((org.infinispan.protostream.impl.SerializationContextImpl) ctx.getSerializationContext()).getMarshallerDelegate(org.infinispan.protostream.WrappedMessage.class);
                  int length = in.readUInt32();
                  int oldLimit = in.pushLimit(length);
                  message = readMessage(delegate, ctx);
                  in.checkLastTagWas(0);
                  in.popLimit(oldLimit);
                  break;
               }
               default: {
                  if (!in.skipField(tag)) done = true;
               }
            }
         }
         if (bytes == null && message == null)
            return EMPTY_INSTANCE;

         if (message != null)
            return new MarshallableObject<>(message.getValue());

         Object userObject = marshaller.objectFromByteBuffer(bytes);
         return new MarshallableObject<>(userObject);
      }

      @Override
      public void write(WriteContext ctx, MarshallableObject wrapper) throws IOException {
         Object object = wrapper.get();
         if (object == null)
            return;

         if (!marshaller.isMarshallableWithoutWrapping(object)) {
            ByteBuffer buf = marshaller.objectToBuffer(object);
            ctx.getWriter().writeBytes(1, buf.getBuf(), buf.getOffset(), buf.getLength());
         } else {
            if (delegate == null)
               delegate = ((org.infinispan.protostream.impl.SerializationContextImpl) ctx.getSerializationContext()).getMarshallerDelegate(WrappedMessage.class);
            writeNestedMessage(delegate, ctx, 2, new WrappedMessage(object));
         }
      }
   }
}
