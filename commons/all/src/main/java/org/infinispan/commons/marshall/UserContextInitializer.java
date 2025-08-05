package org.infinispan.commons.marshall;

import org.infinispan.commons.util.NullValue;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Dan Berindei
 * @since 13.0
 */
@ProtoSchema(
      includeClasses = {
            NullValue.class,
      },
      schemaFileName = "user.commons.proto",
      schemaFilePath = "org/infinispan/commons",
      schemaPackageName = "org.infinispan.commons",
      service = false
)
public interface UserContextInitializer extends SerializationContextInitializer {
}
