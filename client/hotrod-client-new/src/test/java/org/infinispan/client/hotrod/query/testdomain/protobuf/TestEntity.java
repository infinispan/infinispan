package org.infinispan.client.hotrod.query.testdomain.protobuf;


import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class TestEntity {

   @ProtoField(1)
   @Text(projectable = true)
   final String name1;
   @ProtoField(2)
   @Text(projectable = true, analyzer = "simple")
   final String name2;

   @ProtoField(3)
   @Text(projectable = true, analyzer = "whitespace")
   final String name3;

   @ProtoField(4)
   @Text(projectable = true, analyzer = "keyword")
   final String name4;

   @ProtoField(5)
   @Text(projectable = true, analyzer = "stemmer")
   final String name5;

   @ProtoField(6)
   @Text(projectable = true, analyzer = "ngram")
   final String name6;

   @ProtoField(7)
   @Embedded
   final TestEntity child;

   @ProtoFactory
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
}
