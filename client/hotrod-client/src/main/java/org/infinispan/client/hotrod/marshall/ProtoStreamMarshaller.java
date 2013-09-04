package org.infinispan.client.hotrod.marshall;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class ProtoStreamMarshaller extends AbstractMarshaller {

   //todo [anistor] this static field is temporary. we need a way to register the protobuf message marshallers when we configure hotrod client
   private static final SerializationContext serializationContext = ProtobufUtil.newSerializationContext();

   public static SerializationContext getSerializationContext() {
      return serializationContext;
   }

   public ProtoStreamMarshaller() {
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return ProtobufUtil.fromWrappedByteArray(serializationContext, buf, offset, length);
   }

   @Override
   public boolean isMarshallable(Object o) {
      return serializationContext.canMarshall(o.getClass());
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException {
      byte[] bytes = ProtobufUtil.toWrappedByteArray(serializationContext, o);
      return new ByteBuffer(bytes, 0, bytes.length);
   }
}
