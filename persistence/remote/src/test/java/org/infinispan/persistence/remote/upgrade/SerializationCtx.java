package org.infinispan.persistence.remote.upgrade;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = {CustomObject.class},
      schemaFileName = "rolling.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.remote.upgrade")
public interface SerializationCtx extends SerializationContextInitializer {
   SerializationCtx INSTANCE = new SerializationCtxImpl();
}
