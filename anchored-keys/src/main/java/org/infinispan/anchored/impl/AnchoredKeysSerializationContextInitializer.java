package org.infinispan.anchored.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

/**
 * Interface used to initialise a {@link org.infinispan.protostream.SerializationContext} for anchored keys module.
 *
 * @author William Burns
 * @since 16.2
 */
@ProtoSchema(
      includeClasses = {
            ReplaceWithLocationFunction.class
      },
      dependsOn = org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
      schemaFileName = "anchored.keys.proto",
      schemaFilePath = "org/infinispan/anchored",
      schemaPackageName = "org.infinispan.anchored"
)
interface AnchoredKeysSerializationContextInitializer extends SerializationContextInitializer {
}
