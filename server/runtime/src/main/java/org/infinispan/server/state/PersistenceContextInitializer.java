package org.infinispan.server.state;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
@AutoProtoSchemaBuilder(
      includeClasses = {
            ServerStateManagerImpl.IgnoredCaches.class,
            ServerStateManagerImpl.IpFilterRules.class,
            ServerStateManagerImpl.IpFilterRule.class,
      },
      schemaFileName = "persistence.server_state.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.server_state",
      service = false
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
