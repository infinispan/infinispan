package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.api.annotations.indexing.Vector;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Item {

   private String code;

   private byte[] byteVector;

   private float[] floatVector;

   private String buggy;

   @ProtoFactory
   public Item(String code, byte[] byteVector, float[] floatVector, String buggy) {
      this.code = code;
      this.byteVector = byteVector;
      this.floatVector = floatVector;
      this.buggy = buggy;
   }

   @Keyword
   @ProtoField(1)
   public String getCode() {
      return code;
   }

   @Vector(dimension = 3)
   @ProtoField(2)
   public byte[] getByteVector() {
      return byteVector;
   }

   @Vector(dimension = 3)
   @ProtoField(3)
   public float[] getFloatVector() {
      return floatVector;
   }

   @Text
   @ProtoField(4)
   public String getBuggy() {
      return buggy;
   }

   @AutoProtoSchemaBuilder(includeClasses = Item.class)
   public interface ItemSchema extends GeneratedSchema {
      ItemSchema INSTANCE = new ItemSchemaImpl();
   }
}
