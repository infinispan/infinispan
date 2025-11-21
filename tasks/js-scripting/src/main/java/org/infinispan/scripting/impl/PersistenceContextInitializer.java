package org.infinispan.scripting.impl;

import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ProtoSchema(
      dependsOn = org.infinispan.commons.marshall.PersistenceContextInitializer.class,
      includeClasses = {
            ExecutionMode.class,
            ScriptMetadata.class
      },
      schemaFileName = "persistence.scripting.proto",
      schemaFilePath = "org/infinispan/scripting",
      schemaPackageName = "org.infinispan.persistence.scripting",
      syntax = ProtoSyntax.PROTO3,
      service = false,
      orderedMarshallers = true
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
