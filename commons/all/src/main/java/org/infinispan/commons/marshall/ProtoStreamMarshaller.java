package org.infinispan.commons.marshall;

import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.schema.Schema;

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
      initializer.registerSchema(getSerializationContext());
      initializer.registerMarshallers(getSerializationContext());
   }

   public void register(Schema schema, BaseMarshaller... marshallers) {
      SerializationContext serializationContext = this.getSerializationContext();
      FileDescriptorSource fds = FileDescriptorSource.fromString(schema.getName(), schema.toString());
      serializationContext.registerProtoFiles(fds);
      for (BaseMarshaller marshaller : marshallers) {
         serializationContext.registerMarshaller(marshaller);
      }
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
