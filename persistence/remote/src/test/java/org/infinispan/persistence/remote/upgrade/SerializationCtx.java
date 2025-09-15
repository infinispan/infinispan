package org.infinispan.persistence.remote.upgrade;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      includeClasses = {CustomObject.class},
      schemaFileName = "rolling.proto",
      schemaFilePath = "org/infinispan/persistence/remote",
      schemaPackageName = "org.infinispan.persistence.remote.upgrade",
      service = false,
      orderedMarshallers = true
)
public interface SerializationCtx extends SerializationContextInitializer {
   SerializationCtx INSTANCE = new SerializationCtxImpl();
}
