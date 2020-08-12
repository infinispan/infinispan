package org.infinispan.persistence.jdbc.impl;

import org.infinispan.persistence.jdbc.impl.table.AbstractTableManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = AbstractTableManager.MetadataImpl.class,
      schemaFileName = "persistence.jdbc.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.jdbc",
      service = false
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
