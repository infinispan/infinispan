package org.infinispan.server.core;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.commons.marshall.PersistenceContextInitializer.class,
            org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class
      },
      includeClasses = {
            org.infinispan.server.core.transport.NettyTransportConnectionStats.ConnectionAdderTask.class,
            org.infinispan.server.iteration.IterationFilter.class
      },
      schemaFileName = "global.server.core.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.server.core",
      service = false,
      syntax = ProtoSyntax.PROTO3

)
public interface GlobalContextInitializer extends SerializationContextInitializer {
}
