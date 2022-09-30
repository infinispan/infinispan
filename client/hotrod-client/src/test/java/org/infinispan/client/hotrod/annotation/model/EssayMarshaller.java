package org.infinispan.client.hotrod.annotation.model;

import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;

public class EssayMarshaller implements MessageMarshaller<Essay> {

   public static final String PROTO_SCHEMA = "// File name: essay.proto\n" +
         "\n" +
         "syntax = \"proto2\";\n" +
         "\n" +
         "package essay;\n" +
         "\n" +
         "\n" +
         "\n" +
         "message Essay {\n" +
         "   \n" +
         "   optional string title = 1;\n" +
         "   \n" +
         "   optional string content = 2;\n" +
         "}\n" +
         "";

   @Override
   public Essay readFrom(ProtoStreamReader reader) throws IOException {
      Essay essay = new Essay();
      essay.setTitle(reader.readString("title"));
      essay.setContent(reader.readString("content"));
      return essay;
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Essay essay) throws IOException {
      writer.writeString("title", essay.getTitle());
      writer.writeString("content", essay.getContent());
   }

   @Override
   public Class<Essay> getJavaClass() {
      return Essay.class;
   }

   @Override
   public String getTypeName() {
      return "essay.Essay";
   }
}
