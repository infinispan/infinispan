package org.infinispan.query.objectfilter.test.model;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      includeClasses = {
            Address.class,
            Person.class,
            Person.Gender.class,
            PhoneNumber.class
      },
      schemaFileName = "test.object-filter.sampledomain.proto",
      schemaFilePath = "org/infinispan/objectfilter",
      schemaPackageName = "org.infinispan.query.objectfilter.test.model",
      service = false
)
public interface TestDomainSCI extends SerializationContextInitializer {
   SerializationContextInitializer INSTANCE = new TestDomainSCIImpl();
}
