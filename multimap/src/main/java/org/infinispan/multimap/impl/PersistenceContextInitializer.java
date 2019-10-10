package org.infinispan.multimap.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = Bucket.class,
      schemaFileName = "persistence.multimap.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.multimap")
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
