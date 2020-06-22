package org.infinispan.server.logging.events;

import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise the {@link PersistenceMarshaller}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = ServerEventImpl.class,
      schemaFileName = "persistence.event_logger.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.event_logger",
      service = false
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
