package org.infinispan.query.test;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.query.affinity.Entity;
import org.infinispan.query.api.AnotherTestEntity;
import org.infinispan.query.api.NotIndexedType;
import org.infinispan.query.api.TestEntity;
import org.infinispan.query.distributed.NonSerializableKeyType;
import org.infinispan.query.indexedembedded.City;
import org.infinispan.query.indexedembedded.Country;
import org.infinispan.query.queries.faceting.Car;

@AutoProtoSchemaBuilder(
      dependsOn = org.infinispan.test.TestDataSCI.class,
      includeClasses = {
            AnotherGrassEater.class,
            AnotherTestEntity.class,
            Block.class,
            Car.class,
            City.class,
            Country.class,
            CustomKey.class,
            CustomKey2.class,
            CustomKey3.class,
            Entity.class,
            NonSerializableKeyType.class,
            NotIndexedType.class,
            Person.class,
            TestEntity.class,
            Transaction.class,
      },
      schemaFileName = "test.query.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.query")
public interface QueryTestSCI extends SerializationContextInitializer {
   QueryTestSCI INSTANCE = new QueryTestSCIImpl();
}
