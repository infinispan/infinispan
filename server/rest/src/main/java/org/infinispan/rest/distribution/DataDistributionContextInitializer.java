package org.infinispan.rest.distribution;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = {
            CacheDistributionRunnable.class,
            CacheDistributionInfo.class,
            NodeDistributionInfo.class,
      },
      schemaFileName = "persistence.distribution.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.distribution",
      service = false
)
public interface DataDistributionContextInitializer extends SerializationContextInitializer { }
