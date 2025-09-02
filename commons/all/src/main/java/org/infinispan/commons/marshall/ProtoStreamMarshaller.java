package org.infinispan.commons.marshall;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.config.Configuration;

/**
 * Provides the starting point for implementing a {@link org.infinispan.commons.marshall.Marshaller} that uses Protobuf
 * encoding.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class ProtoStreamMarshaller extends ImmutableProtoStreamMarshaller {

   public ProtoStreamMarshaller() {
      this(newSerializationContext());
   }

   public ProtoStreamMarshaller(SerializationContext serializationContext) {
      super(serializationContext);
   }

   public void register(SerializationContextInitializer initializer) {
      initializer.register(getSerializationContext());
   }

   @Override
   public SerializationContext getSerializationContext() {
      return (SerializationContext) serializationContext;
   }

   /**
    * @return a new {@link SerializationContext} with {@link Configuration#wrapCollectionElements()} enabled.
    * @see Configuration.Builder#wrapCollectionElements(boolean)
    */
   public static SerializationContext newSerializationContext() {
      return ProtobufUtil.newSerializationContext(Configuration.builder().wrapCollectionElements(true).build());
   }
}
