package org.infinispan.marshall;

import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

public abstract class AbstractSerializationContextInitializer implements SerializationContextInitializer {

   private final String fileName;

   public AbstractSerializationContextInitializer(String fileName) {
      this.fileName = fileName;
   }

   @Override
   public String getProtoFileName() {
      return fileName;
   }

   @Override
   public String getProtoFile() {
      return FileDescriptorSource.getResourceAsString(getClass(), "/" + fileName);
   }

   @Override
   public void registerSchema(SerializationContext ctx) {
      ctx.registerProtoFiles(FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }

   @Override
   public void registerMarshallers(SerializationContext serCtx) {
   }
}
