package org.infinispan.test.integration.remote.proto;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.test.integration.data.Person;

@ProtoSchema(
      includeClasses = Person.class,
      schemaFileName = "person.proto",
      schemaFilePath = "org/infinispan/test",
      schemaPackageName = "person_sample"
)
public interface PersonSchema extends GeneratedSchema {
   PersonSchema INSTANCE = new PersonSchemaImpl();
}
