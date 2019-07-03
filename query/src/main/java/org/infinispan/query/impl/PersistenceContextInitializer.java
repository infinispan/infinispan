package org.infinispan.query.impl;

import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.query.backend.QueryKnownClasses;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = QueryKnownClasses.KnownClassKey.class,
      schemaFileName = "persistence.query.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.query")
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
