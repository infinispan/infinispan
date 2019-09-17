package org.infinispan.query.test;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "commonIndex")
public class Block {

   @Field(analyze = Analyze.NO)
   private final int height;

   @IndexedEmbedded
   private final Transaction latest;

   @ProtoFactory
   public Block(int height, Transaction latest) {
      this.height = height;
      this.latest = latest;
   }

   @ProtoField(number = 1, defaultValue = "0")
   public int getHeight() {
      return height;
   }

   @ProtoField(number = 2)
   public Transaction getLatest() {
      return latest;
   }

   @Override
   public String toString() {
      return "Block{" +
            "height=" + height +
            ", latest=" + latest +
            '}';
   }
}
