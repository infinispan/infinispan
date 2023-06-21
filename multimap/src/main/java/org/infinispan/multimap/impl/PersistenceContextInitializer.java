package org.infinispan.multimap.impl;

import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = {
            Bucket.class,
            ListBucket.class,
            HashMapBucket.class,
            HashMapBucket.BucketEntry.class,
            SetBucket.class,
            MultimapObjectWrapper.class,
      },
      schemaFileName = "persistence.multimap.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.multimap",
      service = false
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
