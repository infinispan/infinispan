package org.infinispan.test.integration.as;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.test.integration.as.query.Book;

@AutoProtoSchemaBuilder(
      includeClasses = {
            Book.class,
            KeyValueEntity.class
      },
      schemaFileName = "test.it.wildfly.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.it.wildfly")
public interface WidlflyIntegrationSCI extends SerializationContextInitializer {
      Class[] CLASSES = new Class[] {Book.class, KeyValueEntity.class,
            WidlflyIntegrationSCI.class, WidlflyIntegrationSCIImpl.class};
      String RESOURCE = "proto/generated/test.it.wildfly.proto";
}
