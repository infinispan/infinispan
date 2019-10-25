package org.infinispan.server.core;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = CacheIgnoreManager.IgnoredCaches.class,
      schemaFileName = "persistence.server.core.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.server.core")
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
