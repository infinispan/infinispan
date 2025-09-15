package org.infinispan.persistence.jdbc.impl;

import org.infinispan.persistence.jdbc.impl.table.AbstractTableManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
@ProtoSchema(
      includeClasses = AbstractTableManager.MetadataImpl.class,
      schemaFileName = "persistence.jdbc.proto",
      schemaFilePath = "org/infinispan/persistence/jdbc",
      schemaPackageName = "org.infinispan.persistence.jdbc",
      service = false,
      orderedMarshallers = true
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
