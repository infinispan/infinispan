package org.infinispan.protostream.sampledomain;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Vector;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Image {

   private String name;
   private byte[] byteEmbedding;
   private float[] floatEmbedding;

   @ProtoFactory
   public Image(String name, byte[] byteEmbedding, float[] floatEmbedding) {
      this.name = name;
      this.byteEmbedding = byteEmbedding;
      this.floatEmbedding = floatEmbedding;
   }

   @Keyword
   @ProtoField(1)
   public String getName() {
      return name;
   }

   @Vector(dimension = 50)
   @ProtoField(2)
   public byte[] getByteEmbedding() {
      return byteEmbedding;
   }

   @Vector(dimension = 50)
   @ProtoField(3)
   public float[] getFloatEmbedding() {
      return floatEmbedding;
   }

   @AutoProtoSchemaBuilder(schemaPackageName = "query.domain", includeClasses = Image.class, schemaFilePath = "/", schemaFileName = "image.proto")
   public interface ImageSchema extends GeneratedSchema {
      ImageSchema INSTANCE = new ImageSchemaImpl();
   }
}
