package org.infinispan.spring.common.session;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Dan Berindei
 * @since 12.1
 */
@AutoProtoSchemaBuilder(
      includeClasses = {
            MapSessionProtoAdapter.class,
            MapSessionProtoAdapter.SessionAttribute.class,
      },
      schemaFileName = "persistence.spring5.session.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.spring.session",
      service = false
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
