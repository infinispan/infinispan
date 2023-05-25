package org.infinispan.server.resp.test;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;

@AutoProtoSchemaBuilder(dependsOn = TestDataSCI.class, includeClasses = {
            Person.class,
            SuperPerson.class
}, schemaFileName = "test.resp.proto", schemaFilePath = "proto/generated", schemaPackageName = "org.infinispan.server.resp.test", service = false)
interface MultimapSCI extends SerializationContextInitializer {
      MultimapSCI INSTANCE = new MultimapSCIImpl();
}
