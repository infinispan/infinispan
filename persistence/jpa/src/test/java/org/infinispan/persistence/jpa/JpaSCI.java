package org.infinispan.persistence.jpa;

import org.infinispan.persistence.jpa.entity.Address;
import org.infinispan.persistence.jpa.entity.Document;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.infinispan.persistence.jpa.entity.Person;
import org.infinispan.persistence.jpa.entity.User;
import org.infinispan.persistence.jpa.entity.Vehicle;
import org.infinispan.persistence.jpa.entity.VehicleId;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = {
            Address.class,
            Document.class,
            KeyValueEntity.class,
            Person.class,
            User.class,
            Vehicle.class,
            VehicleId.class,
      },
      schemaFileName = "test.jpa.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.jpa")
public interface JpaSCI extends SerializationContextInitializer {
      JpaSCI INSTANCE = new JpaSCIImpl();
}
