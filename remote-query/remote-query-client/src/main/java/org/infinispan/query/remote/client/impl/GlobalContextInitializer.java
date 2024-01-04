package org.infinispan.query.remote.client.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.query.remote.client.FilterResult;

@AutoProtoSchemaBuilder(
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
      service = false
)
interface GlobalContextInitializer extends SerializationContextInitializer {
      GlobalContextInitializer INSTANCE = new GlobalContextInitializerImpl();
}
