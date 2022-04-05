package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@ProtoDoc("@Indexed")
public class KeywordEntity {

   private final String keyword;

   @ProtoFactory
   public KeywordEntity(String keyword) {
      this.keyword = keyword;
   }

   @ProtoField(value = 1, required = true)
   @ProtoDoc("@Field(store = Store.YES, analyze = Analyze.YES, analyzer = @Analyzer(definition = \"keyword\"))")
   public String getKeyword() {
      return keyword;
   }

   @AutoProtoSchemaBuilder(includeClasses = KeywordEntity.class)
   public interface KeywordSchema extends GeneratedSchema {
      KeywordSchema INSTANCE = new KeywordSchemaImpl();
   }
}
