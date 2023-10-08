package org.infinispan.server;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.tasks.api.impl.GlobalContextInitializer.class,
            org.infinispan.tasks.impl.GlobalContextInitializer.class
      },
      includeClasses = {
            org.infinispan.server.state.ServerStateManagerImpl.IgnoredCaches.class,
            org.infinispan.server.state.ServerStateManagerImpl.IpFilterRules.class,
            org.infinispan.server.state.ServerStateManagerImpl.IpFilterRule.class,
            org.infinispan.server.tasks.DistributedServerTask.class,
            ExitStatus.class,
            ExitStatus.ExitMode.class,
            Server.ShutdownRunnable.class,
      },
      schemaFileName = "global.server.runtime.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.server.runtime",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
interface GlobalContextInitializer extends SerializationContextInitializer {
}
