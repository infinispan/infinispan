package org.infinispan.commons.marshall;

import org.infinispan.commons.util.NullValue;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} using the specified Pojos,
 * Marshaller implementations and provided .proto schemas.
 *
 * @author Dan Berindei
 * @since 13.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = {
            NullValue.class,
      },
      schemaFileName = "user.commons.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.commons",
      service = false
)
public interface UserContextInitializer extends SerializationContextInitializer {
}
