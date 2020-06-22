package org.infinispan.multimap.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;

@AutoProtoSchemaBuilder(
      dependsOn = TestDataSCI.class,
      includeClasses = {
            Person.class,
            SuperPerson.class
      },
      schemaFileName = "test.multimap.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.multimap",
      service = false
)
interface MultimapSCI extends SerializationContextInitializer {
   MultimapSCI INSTANCE = new MultimapSCIImpl();
}
