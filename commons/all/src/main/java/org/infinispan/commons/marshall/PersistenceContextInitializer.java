package org.infinispan.commons.marshall;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ProtoSchema(
      includeClasses = {
            MediaType.class,
            WrappedByteArray.class
      },
      schemaFileName = "persistence.commons.proto",
      schemaFilePath = "org/infinispan/commons",
      schemaPackageName = "org.infinispan.persistence.commons",
      service = false
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
