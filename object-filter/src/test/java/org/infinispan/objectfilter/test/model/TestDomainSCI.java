package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = {
            Address.class,
            Person.class,
            Person.Gender.class,
            PhoneNumber.class
      },
      schemaFileName = "test.object-filter.sampledomain.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.objectfilter.test.model",
      service = false
)
public interface TestDomainSCI extends SerializationContextInitializer {
   SerializationContextInitializer INSTANCE = new TestDomainSCIImpl();
}
