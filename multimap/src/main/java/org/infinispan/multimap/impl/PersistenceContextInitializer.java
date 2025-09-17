package org.infinispan.multimap.impl;

import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = {
            Bucket.class,
            ListBucket.class,
            HashMapBucket.class,
            HashMapBucket.BucketEntry.class,
            MultimapObjectWrapper.class,
            SetBucket.class,
            SortedSetBucket.class,
            ScoredValue.class,
            SortedSetBucket.IndexValue.class,
      },
      schemaFileName = "persistence.multimap.proto",
      schemaFilePath = "org/infinispan/multimap",
      schemaPackageName = "org.infinispan.persistence.multimap",
      service = false,
      orderedMarshallers = true
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
