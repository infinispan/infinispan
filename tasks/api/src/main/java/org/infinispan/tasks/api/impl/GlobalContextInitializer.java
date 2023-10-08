package org.infinispan.tasks.api.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.tasks.TaskContext;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = TaskContext.class,
      schemaFileName = "global.tasks.api.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.tasks.api",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer {
}
