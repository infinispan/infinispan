package org.infinispan.test;

import org.infinispan.distribution.MagicKey;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.BrokenMarshallingPojo;
import org.infinispan.test.data.CountMarshallingPojo;
import org.infinispan.test.data.DelayedMarshallingPojo;
import org.infinispan.test.data.Key;
import org.infinispan.test.data.Person;
import org.infinispan.test.data.Value;

@AutoProtoSchemaBuilder(
      // TODO revaluate use of Person where Value is more appropriate
      includeClasses = {
            Address.class,
            BrokenMarshallingPojo.class,
            DelayedMarshallingPojo.class,
            Key.class,
            MagicKey.class,
            CountMarshallingPojo.class,
            Person.class,
            Value.class
      },
      schemaFileName = "test.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.core")
public interface TestDataSCI extends SerializationContextInitializer {
   TestDataSCI INSTANCE = new TestDataSCIImpl();
}
