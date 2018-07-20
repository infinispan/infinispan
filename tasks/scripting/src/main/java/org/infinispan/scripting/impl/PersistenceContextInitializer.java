package org.infinispan.scripting.impl;

import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.commons.marshall.PersistenceContextInitializer.class,
      includeClasses = {
            ExecutionMode.class,
            ScriptMetadata.class
      },
      schemaFileName = "persistence.scripting.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.scripting")
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
