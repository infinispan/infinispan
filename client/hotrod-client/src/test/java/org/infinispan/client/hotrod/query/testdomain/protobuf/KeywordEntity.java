package org.infinispan.client.hotrod.query.testdomain.protobuf;

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
}
