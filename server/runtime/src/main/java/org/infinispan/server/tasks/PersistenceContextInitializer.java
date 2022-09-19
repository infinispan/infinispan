package org.infinispan.server.tasks;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.security.impl.SubjectAdapter;
import org.infinispan.tasks.TaskContext;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Tristan Tarrant
 * @since 11.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = {
            SubjectAdapter.class,
            TaskContext.class,
            DistributedServerTask.class
      },
      schemaFileName = "persistence.servertasks.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.servertasks",
      service = false
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
