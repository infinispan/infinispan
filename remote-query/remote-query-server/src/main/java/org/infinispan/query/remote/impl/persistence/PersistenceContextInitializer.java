package org.infinispan.query.remote.impl.persistence;

import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = ProtobufValueWrapper.class,
      schemaFileName = "persistence.remote-query.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.remote-query")
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
