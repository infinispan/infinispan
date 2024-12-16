package org.infinispan.rest;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.rest.distribution.CacheDistributionInfo;
import org.infinispan.rest.distribution.KeyDistributionInfo;
import org.infinispan.rest.distribution.NodeDistributionInfo;

@ProtoSchema(
      allowNullFields = true,
      includeClasses = {
            CacheDistributionInfo.class,
            NodeDistributionInfo.class,
            KeyDistributionInfo.class,
      },
      schemaFileName = "global.server.rest.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.server.rest",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer { }
