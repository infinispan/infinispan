package org.infinispan.commons.marshall;

import java.io.IOException;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * Provides the starting point for implementing a {@link org.infinispan.commons.marshall.Marshaller} that uses Protobuf
 * encoding.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class ProtoStreamMarshaller extends AbstractMarshaller {

   private final SerializationContext serializationContext;

   public ProtoStreamMarshaller() {
      this(ProtobufUtil.newSerializationContext());
   }

   public ProtoStreamMarshaller(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   public void register(SerializationContextInitializer initializer) {
      initializer.registerSchema(serializationContext);
      initializer.registerMarshallers(serializationContext);
   }

   /**
    * @return the SerializationContext instance to use
    */
   public SerializationContext getSerializationContext() {
      return serializationContext;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return ProtobufUtil.fromWrappedByteArray(getSerializationContext(), buf, offset, length);
   }

   @Override
   public boolean isMarshallable(Object o) {
      // our marshaller can handle all of these primitive/scalar types as well even if we do not
      // have a per-type marshaller defined in our SerializationContext
      return o instanceof String ||
            o instanceof Long ||
            o instanceof Integer ||
            o instanceof Double ||
            o instanceof Float ||
            o instanceof Boolean ||
            o instanceof byte[] ||
            o instanceof Byte ||
            o instanceof Short ||
            o instanceof Character ||
            o instanceof java.util.Date ||
            o instanceof java.time.Instant ||
            getSerializationContext().canMarshall(o.getClass());
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException {
      byte[] bytes = ProtobufUtil.toWrappedByteArray(getSerializationContext(), o);
      return new ByteBufferImpl(bytes, 0, bytes.length);
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_PROTOSTREAM;
   }
}
