package org.infinispan.protostream.sampledomain;

import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Vector;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

import java.util.List;

@Indexed
public class KeywordVector {

   final String name;
   final byte[] byteEmbedding;
   final float[] floatEmbedding;
   final List<Metadata> metadata;

   @ProtoFactory
   public KeywordVector(String name, byte[] byteEmbedding, float[] floatEmbedding, List<Metadata> metadata) {
      this.name = name;
      this.byteEmbedding = byteEmbedding;
      this.floatEmbedding = floatEmbedding;
      this.metadata = metadata;
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

   @Embedded
   @ProtoField(4)
   public List<Metadata> getMetadata() {
      return metadata;
   }
}
