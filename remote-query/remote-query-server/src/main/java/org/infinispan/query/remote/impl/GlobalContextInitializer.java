package org.infinispan.query.remote.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
            org.infinispan.query.remote.client.impl.GlobalContextInitializer.class
      },
      includeClasses = {
            org.infinispan.query.remote.impl.filter.IckleBinaryProtobufFilterAndConverter.class,
            org.infinispan.query.remote.impl.filter.IckleContinuousQueryProtobufCacheEventFilterConverter.class,
            org.infinispan.query.remote.impl.filter.IckleProtobufCacheEventFilterConverter.class,
            org.infinispan.query.remote.impl.filter.IckleProtobufFilterAndConverter.class,
      },
      schemaFileName = "global.remote.query.server.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.remote.query.server",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
interface GlobalContextInitializer extends SerializationContextInitializer {
}
