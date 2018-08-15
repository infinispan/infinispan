package org.infinispan.client.hotrod.query.testdomain.protobuf;


import java.io.IOException;

import org.infinispan.protostream.MessageMarshaller;

public class TestEntity {

   private final String name1;
   private final String name2;
   private final String name3;
   private final String name4;
   private final String name5;
   private final String name6;

   public TestEntity(String name1, String name2, String name3, String name4, String name5, String name6) {
      this.name1 = name1;
      this.name2 = name2;
      this.name3 = name3;
      this.name4 = name4;
      this.name5 = name5;
      this.name6 = name6;
   }

   public static class TestEntityMarshaller implements MessageMarshaller<TestEntity> {

      @Override
      public TestEntity readFrom(ProtoStreamReader reader) throws IOException {
         return new TestEntity(reader.readString("name1"), reader.readString("name2"),
               reader.readString("name3"), reader.readString("name4"),
               reader.readString("name5"), reader.readString("name6"));
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, TestEntity testEntity) throws IOException {
         writer.writeString("name1", testEntity.name1);
         writer.writeString("name2", testEntity.name2);
         writer.writeString("name3", testEntity.name3);
         writer.writeString("name4", testEntity.name4);
         writer.writeString("name5", testEntity.name5);
         writer.writeString("name6", testEntity.name6);
      }

      @Override
      public Class<? extends TestEntity> getJavaClass() {
         return TestEntity.class;
      }

      @Override
      public String getTypeName() {
         return "TestEntity";
      }
   }
}
