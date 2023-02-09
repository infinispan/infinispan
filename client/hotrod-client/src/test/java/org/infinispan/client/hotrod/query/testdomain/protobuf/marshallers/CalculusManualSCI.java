package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.UncheckedIOException;

import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

public class CalculusManualSCI implements SerializationContextInitializer {

   @Override
   public String getProtoFileName() {
      return "calculus-manual.proto";
   }

   @Override
   public String getProtoFile() throws UncheckedIOException {
      return FileDescriptorSource.getResourceAsString(getClass(), "/protostream/" + getProtoFileName());
   }

   @Override
   public void registerSchema(SerializationContext serCtx) {
      serCtx.registerProtoFiles(FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }

   @Override
   public void registerMarshallers(SerializationContext serCtx) {
      serCtx.registerMarshaller(new CalculusManualMarshaller());
   }
}
