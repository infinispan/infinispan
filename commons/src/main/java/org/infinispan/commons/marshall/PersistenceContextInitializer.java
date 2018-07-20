package org.infinispan.commons.marshall;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = {
            MediaType.class,
            WrappedByteArray.class
      },
      schemaFileName = "persistence.commons.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.commons")
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
