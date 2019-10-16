package org.infinispan.server.logging.events;

import java.io.UncheckedIOException;
import java.util.UUID;

import org.infinispan.marshall.protostream.impl.marshallers.UUIDMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

public class PersistenceContextManualInitializer implements SerializationContextInitializer {

   private static String type(String message) {
      return String.format("org.infinispan.persistence.m.event_logger.%s", message);
   }

   @Override
   public String getProtoFileName() {
      return "persistence.m.event_logger.proto";
   }

   @Override
   public String getProtoFile() throws UncheckedIOException {
      return FileDescriptorSource.getResourceAsString(getClass(), "/proto/" + getProtoFileName());
   }

   @Override
   public void registerSchema(SerializationContext serCtx) {
      serCtx.registerProtoFiles(org.infinispan.protostream.FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }

   @Override
   public void registerMarshallers(SerializationContext serCtx) {
      serCtx.registerMarshaller(new UUIDMarshaller(type(UUID.class.getSimpleName())));
   }
}
