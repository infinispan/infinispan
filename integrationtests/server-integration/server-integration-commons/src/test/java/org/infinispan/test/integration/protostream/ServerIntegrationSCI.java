package org.infinispan.test.integration.protostream;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.test.integration.data.KeyValueEntity;

@AutoProtoSchemaBuilder(
      includeClasses = {
            KeyValueEntity.class
      },
      schemaFileName = "test.it.proto",
      schemaPackageName = "server_integration",
      service = false
)
public interface ServerIntegrationSCI extends SerializationContextInitializer {
      Class[] CLASSES = new Class[] {KeyValueEntity.class,
            ServerIntegrationSCI.class, ServerIntegrationSCIImpl.class};
}
