package org.infinispan.query.core.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      dependsOn = org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
      includeClasses = {
            org.infinispan.query.core.impl.EmbeddedQuery.DeleteFunction.class,
            org.infinispan.query.core.impl.continuous.ContinuousQueryResult.class,
            org.infinispan.query.core.impl.continuous.ContinuousQueryResult.ResultType.class,
            org.infinispan.query.core.impl.continuous.IckleContinuousQueryCacheEventFilterConverter.class,
            org.infinispan.query.core.impl.eventfilter.IckleCacheEventFilterConverter.class,
            org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter.class,
      },
      schemaFileName = "global.query.core.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.query.core",
      service = false,
      syntax = ProtoSyntax.PROTO3
)
public interface GlobalContextInitializer extends SerializationContextInitializer {
}
