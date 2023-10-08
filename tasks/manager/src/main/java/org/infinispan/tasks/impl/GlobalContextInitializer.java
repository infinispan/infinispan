package org.infinispan.tasks.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoSchema(
      allowNullFields = true,
      dependsOn = org.infinispan.protostream.types.java.CommonTypes.class,
      includeClasses = TaskExecutionImpl.class,
      schemaFileName = "global.tasks.manager.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.tasks.manager",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer {
}
