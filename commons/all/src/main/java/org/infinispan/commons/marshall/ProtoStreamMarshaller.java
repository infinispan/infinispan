package org.infinispan.commons.marshall;

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
public class ProtoStreamMarshaller extends ImmutableProtoStreamMarshaller {

   public ProtoStreamMarshaller() {
      this(ProtobufUtil.newSerializationContext());
   }

   public ProtoStreamMarshaller(SerializationContext serializationContext) {
      super(serializationContext);
   }

   public void register(SerializationContextInitializer initializer) {
      initializer.registerSchema(getSerializationContext());
      initializer.registerMarshallers(getSerializationContext());
   }

   @Override
   public SerializationContext getSerializationContext() {
      return (SerializationContext) serializationContext;
   }
}
