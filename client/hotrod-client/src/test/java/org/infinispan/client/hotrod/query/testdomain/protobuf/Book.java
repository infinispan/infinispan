package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@ProtoDoc("@Indexed")
public class Book {

   private final String title;

   @ProtoFactory
   public Book(String title) {
      this.title = title;
   }

   @ProtoField(value = 1)
   @ProtoDoc("@Field(store = Store.YES, analyze = Analyze.YES, analyzer = @Analyzer(definition = \"lowercase\"))")
   public String getTitle() {
      return title;
   }

   @AutoProtoSchemaBuilder(includeClasses = Book.class)
   public interface Schema extends GeneratedSchema {
      Schema INSTANCE = new SchemaImpl();
   }
}
