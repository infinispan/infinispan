package org.infinispan.client.hotrod.query.testdomain.protobuf;


import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;

public class KeywordEntity {

   private final String keyword;

   public KeywordEntity(String keyword) {
      this.keyword = keyword;
   }

   public static class Marshaller implements MessageMarshaller<KeywordEntity> {

      @Override
      public KeywordEntity readFrom(ProtoStreamReader reader) throws IOException {
         String keyword = reader.readString("keyword");
         return new KeywordEntity(keyword);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, KeywordEntity entity) throws IOException {
         writer.writeString("keyword", entity.keyword);
      }

      @Override
      public Class<KeywordEntity> getJavaClass() {
         return KeywordEntity.class;
      }

      @Override
      public String getTypeName() {
         return "KeywordEntity";
      }
   }
}
