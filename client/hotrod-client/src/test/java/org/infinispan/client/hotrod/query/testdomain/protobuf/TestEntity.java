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
   private final TestEntity child;

   public TestEntity(String name1, String name2, String name3, String name4, String name5, String name6, TestEntity child) {
      this.name1 = name1;
      this.name2 = name2;
      this.name3 = name3;
      this.name4 = name4;
      this.name5 = name5;
      this.name6 = name6;
      this.child = child;
   }

   public TestEntity(String name1, String name2, String name3, String name4, String name5, String name6) {
      this(name1, name2, name3, name4, name5, name6, null);
   }

   public static class TestEntityMarshaller implements MessageMarshaller<TestEntity> {

      @Override
      public TestEntity readFrom(ProtoStreamReader reader) throws IOException {
         String name1 = reader.readString("name1");
         String name2 = reader.readString("name2");
         String name3 = reader.readString("name3");
         String name4 = reader.readString("name4");
         String name5 = reader.readString("name5");
         String name6 = reader.readString("name6");
         TestEntity child = reader.readObject("child", TestEntity.class);
         return new TestEntity(name1, name2, name3, name4, name5, name6, child);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, TestEntity testEntity) throws IOException {
         writer.writeString("name1", testEntity.name1);
         writer.writeString("name2", testEntity.name2);
         writer.writeString("name3", testEntity.name3);
         writer.writeString("name4", testEntity.name4);
         writer.writeString("name5", testEntity.name5);
         writer.writeString("name6", testEntity.name6);
         writer.writeObject("child", testEntity.child, TestEntity.class);
      }

      @Override
      public Class<TestEntity> getJavaClass() {
         return TestEntity.class;
      }

      @Override
      public String getTypeName() {
         return "TestEntity";
      }
   }
}
