package org.infinispan.test.integration.remote.proto;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.test.integration.data.Book;

@ProtoSchema(
      includeClasses = Book.class,
      schemaFileName = "book.proto",
      schemaFilePath = "org/infinispan/test",
      schemaPackageName = "book_sample"
)
public interface BookQuerySchema extends GeneratedSchema {
   BookQuerySchema INSTANCE = new BookQuerySchemaImpl();
}
