package org.infinispan.server.memcached;

import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = MemcachedMetadata.class,
      schemaFileName = "persistence.memcached.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.memcached",
      service = false
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
