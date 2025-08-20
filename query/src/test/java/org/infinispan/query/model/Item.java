package org.infinispan.query.model;

import java.util.List;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.api.annotations.indexing.Vector;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
public class Item {

   private final String code;

   private final byte[] byteVector;

   private final float[] floatVector;

   private final String buggy;

   private final Integer ordinal;

   private final List<Metadata> metadata;

   @ProtoFactory
   public Item(String code, byte[] byteVector, float[] floatVector, String buggy, Integer ordinal, List<Metadata> metadata) {
      this.code = code;
      this.byteVector = byteVector;
      this.floatVector = floatVector;
      this.buggy = buggy;
      this.ordinal = ordinal;
      this.metadata = metadata;
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

   @Basic(sortable = true)
   @ProtoField(5)
   public Integer getOrdinal() {
      return ordinal;
   }

   @ProtoField(6)
   @Embedded
   public List<Metadata> getMetadata() {
      return metadata;
   }

   @ProtoSchema(
         includeClasses = {Item.class, Metadata.class},
         service = false
   )
   public interface ItemSchema extends GeneratedSchema {
      ItemSchema INSTANCE = new ItemSchemaImpl();
   }
}
