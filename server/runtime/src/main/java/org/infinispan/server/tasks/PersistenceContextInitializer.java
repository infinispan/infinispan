package org.infinispan.server.tasks;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Tristan Tarrant
 * @since 11.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = {
            TaskParameter.class,
            DistributedServerTask.class
      },
      schemaFileName = "persistence.servertasks.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.servertasks")
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
