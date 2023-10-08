package org.infinispan.server;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      includeClasses = {
            ExitStatus.class,
            ExitStatus.ExitMode.class,
            Server.ShutdownRunnable.class
      },
      schemaFileName = "global.server.runtime.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.server.runtime",
      service = false,
      syntax = ProtoSyntax.PROTO3

)
interface GlobalContextInitializer extends SerializationContextInitializer {
}
