package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
public class Book {

   private final String title;

   @ProtoFactory
   public Book(String title) {
      this.title = title;
   }

   @ProtoField(value = 1)
   @Keyword(projectable = true, normalizer = "lowercase")
   public String getTitle() {
      return title;
   }

   @ProtoSchema(
         includeClasses = Book.class,
         service = false
   )
   public interface BookSchema extends GeneratedSchema {
      BookSchema INSTANCE = new BookSchemaImpl();
   }
}
