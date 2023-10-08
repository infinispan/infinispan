package org.infinispan.rest;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.rest.search.entity.Address;
import org.infinispan.rest.search.entity.Gender;
import org.infinispan.rest.search.entity.Person;
import org.infinispan.rest.search.entity.PhoneNumber;

@ProtoSchema(
      dependsOn = {
            org.infinispan.query.remote.impl.persistence.PersistenceContextInitializer.class,
            org.infinispan.rest.GlobalContextInitializer.class,
            org.infinispan.server.core.GlobalContextInitializer.class
      },
      includeClasses = {
            Address.class,
            Gender.class,
            Person.class,
            PhoneNumber.class,
            TestClass.class
      },
      schemaFileName = "test.rest.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.rest",
      service = false
)
public interface RestTestSCI extends SerializationContextInitializer {
   RestTestSCI INSTANCE = new RestTestSCIImpl();
}
