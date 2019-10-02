package org.infinispan.rest;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.rest.search.entity.Address;
import org.infinispan.rest.search.entity.Gender;
import org.infinispan.rest.search.entity.Person;
import org.infinispan.rest.search.entity.PhoneNumber;

@AutoProtoSchemaBuilder(
      includeClasses = {
            Address.class,
            Gender.class,
            Person.class,
            PhoneNumber.class,
            TestClass.class
      },
      schemaFileName = "test.rest.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.rest")
public interface RestTestSCI extends SerializationContextInitializer {
   RestTestSCI INSTANCE = new RestTestSCIImpl();
}
