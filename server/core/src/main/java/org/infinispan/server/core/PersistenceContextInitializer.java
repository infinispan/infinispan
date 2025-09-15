package org.infinispan.server.core;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.server.core.backup.resources.CacheResource;
import org.infinispan.server.core.backup.resources.CounterResource;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.commons.marshall.PersistenceContextInitializer.class,
            org.infinispan.counter.api._private.PersistenceContextInitializer.class,
            org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class
      },
      includeClasses = {
            CacheResource.CacheBackupEntry.class,
            CounterResource.CounterBackupEntry.class
      },
      schemaFileName = "persistence.server.core.proto",
      schemaFilePath = "org/infinispan/server/core",
      schemaPackageName = "org.infinispan.persistence.server.core",
      service = false,
      orderedMarshallers = true
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
