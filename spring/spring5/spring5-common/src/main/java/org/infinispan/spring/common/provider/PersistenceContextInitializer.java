package org.infinispan.spring.common.provider;

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
            NullValue.class,
      },
      schemaFileName = "persistence.spring5.provider.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.spring.provider",
      service = false
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
