package org.infinispan.client.hotrod.marshall;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;

@AutoProtoSchemaBuilder(
      includeClasses = NotIndexed.class,
      schemaPackageName = "sample_bank_account",
      schemaFileName = "not_indexed.proto",
      schemaFilePath = "/",
      service = false
)
public interface NotIndexedSchema extends GeneratedSchema {

   GeneratedSchema INSTANCE = new NotIndexedSchemaImpl();
}
