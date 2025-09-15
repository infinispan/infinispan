package org.infinispan.server.core.query.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.server.core.query.impl.filter.IckleBinaryProtobufFilterAndConverter;
import org.infinispan.server.core.query.impl.filter.IckleContinuousQueryProtobufCacheEventFilterConverter;
import org.infinispan.server.core.query.impl.filter.IckleProtobufCacheEventFilterConverter;
import org.infinispan.server.core.query.impl.filter.IckleProtobufFilterAndConverter;
import org.infinispan.server.core.query.impl.persistence.PersistenceContextInitializer;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
            org.infinispan.query.core.impl.GlobalContextInitializer.class,
            org.infinispan.query.remote.client.impl.GlobalContextInitializer.class,
            PersistenceContextInitializer.class
      },
      includeClasses = {
            IckleBinaryProtobufFilterAndConverter.class,
            IckleContinuousQueryProtobufCacheEventFilterConverter.class,
            IckleProtobufCacheEventFilterConverter.class,
            IckleProtobufFilterAndConverter.class,
      },
      schemaFileName = "global.remote.query.server.proto",
      schemaFilePath = "org/infinispan/query/server",
      schemaPackageName = "org.infinispan.global.remote.query.server",
      service = false,
      syntax = ProtoSyntax.PROTO3,
      orderedMarshallers = true
)
public interface GlobalContextInitializer extends SerializationContextInitializer {
      SerializationContextInitializer INSTANCE = new GlobalContextInitializerImpl();
}
