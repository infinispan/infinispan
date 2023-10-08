package org.infinispan.scripting.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            PersistenceContextInitializer.class,
            org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
      },
      includeClasses = DistributedScript.class,
      schemaFileName = "global.scripting.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.scripting",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
interface GlobalContextInitializer extends SerializationContextInitializer {
}
