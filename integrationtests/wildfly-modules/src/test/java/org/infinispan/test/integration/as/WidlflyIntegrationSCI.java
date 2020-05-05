package org.infinispan.test.integration.as;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = {
            KeyValueEntity.class
      },
      schemaFileName = "test.it.wildfly.proto",
      schemaPackageName = "org.infinispan.test.it.wildfly")
public interface WidlflyIntegrationSCI extends SerializationContextInitializer {
      Class[] CLASSES = new Class[] {KeyValueEntity.class,
            WidlflyIntegrationSCI.class, WidlflyIntegrationSCIImpl.class};
}
