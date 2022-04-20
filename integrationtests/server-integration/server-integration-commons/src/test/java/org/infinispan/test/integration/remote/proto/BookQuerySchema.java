package org.infinispan.test.integration.remote.proto;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.test.integration.data.Book;

@AutoProtoSchemaBuilder(
      includeClasses = {
            Book.class
      },
      schemaFileName = "book.proto",
      schemaFilePath = "proto/",
      schemaPackageName = "book_sample")
public interface BookQuerySchema extends GeneratedSchema {
}
