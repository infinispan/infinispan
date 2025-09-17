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
      schemaFilePath = "org/infinispan/tasks/api",
      schemaPackageName = "org.infinispan.global.tasks.api",
      service = false,
      syntax = ProtoSyntax.PROTO3,
      orderedMarshallers = true
)
public interface GlobalContextInitializer extends SerializationContextInitializer {
}
