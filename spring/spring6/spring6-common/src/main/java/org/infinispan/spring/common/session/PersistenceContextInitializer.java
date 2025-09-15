package org.infinispan.spring.common.session;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Dan Berindei
 * @since 12.1
 */
@ProtoSchema(
      includeClasses = {
            MapSessionProtoAdapter.class,
            MapSessionProtoAdapter.SessionAttribute.class,
            SessionUpdateRemappingFunctionProtoAdapter.class,
      },
      schemaFileName = "persistence.spring6.session.proto",
      schemaFilePath = "org/infinispan/spring/common",
      schemaPackageName = "org.infinispan.persistence.spring.session",
      service = false,
      orderedMarshallers = true
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
