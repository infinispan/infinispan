package org.infinispan.query.remote.client.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.query.remote.client.FilterResult;

@ProtoSchema(
      includeClasses = {
            ContinuousQueryResult.class,
            ContinuousQueryResult.ResultType.class,
            FilterResult.class,
            QueryRequest.class,
            QueryResponse.class
      },
      schemaFileName = "query.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.query.remote.client",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer {
      GlobalContextInitializer INSTANCE = new GlobalContextInitializerImpl();
}
