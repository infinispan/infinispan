package org.infinispan.query.test;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.mapper.pojo.automaticindexing.ReindexOnUpdate;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.IndexingDependency;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "blockIndex")
public class Block implements Serializable {

   @Field(analyze = Analyze.NO)
   private final int height;

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

   @IndexedEmbedded
   @IndexingDependency(reindexOnUpdate = ReindexOnUpdate.NO)
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
