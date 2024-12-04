package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
public class KeywordEntity {

   private final String keyword;

   @ProtoFactory
   public KeywordEntity(String keyword) {
      this.keyword = keyword;
   }

   @ProtoField(value = 1)
   @Text(projectable = true, analyzer = "keyword")
   public String getKeyword() {
      return keyword;
   }

   @ProtoSchema(includeClasses = KeywordEntity.class, service = false)
   public interface KeywordSchema extends GeneratedSchema {
      KeywordSchema INSTANCE = new KeywordSchemaImpl();
   }
}
