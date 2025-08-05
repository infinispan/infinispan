package org.infinispan.server.resp.test;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;

@ProtoSchema(
      dependsOn = TestDataSCI.class,
      includeClasses = {
            Person.class,
            SuperPerson.class
      },
      schemaFileName = "test.resp.proto",
      schemaFilePath = "org/infinispan/server/resp",
      schemaPackageName = "org.infinispan.server.resp.test",
      service = false
)
interface MultimapSCI extends SerializationContextInitializer {
      MultimapSCI INSTANCE = new MultimapSCIImpl();
}
