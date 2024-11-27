package org.infinispan.persistence.remote.upgrade;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      includeClasses = {CustomObject.class},
      schemaFileName = "rolling.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.remote.upgrade",
      service = false
)
public interface SerializationCtx extends SerializationContextInitializer {
   SerializationCtx INSTANCE = new SerializationCtxImpl();
}
