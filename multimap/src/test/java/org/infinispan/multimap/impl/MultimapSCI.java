package org.infinispan.multimap.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.test.TestDataSCI;

@AutoProtoSchemaBuilder(
      dependsOn = TestDataSCI.class,
      includeClasses = SuperPerson.class,
      schemaFileName = "test.multimap.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.multimap")
interface MultimapSCI extends SerializationContextInitializer {
   MultimapSCI INSTANCE = new MultimapSCIImpl();
}
