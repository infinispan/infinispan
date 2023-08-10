package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class KeywordEntity {

   private final String keyword;

   @ProtoFactory
   public KeywordEntity(String keyword) {
      this.keyword = keyword;
   }

   @ProtoField(value = 1, required = true)
   @Text(projectable = true, analyzer = "keyword")
   public String getKeyword() {
      return keyword;
   }

   @AutoProtoSchemaBuilder(includeClasses = KeywordEntity.class)
   public interface KeywordSchema extends GeneratedSchema {
      KeywordSchema INSTANCE = new KeywordSchemaImpl();
   }
}
