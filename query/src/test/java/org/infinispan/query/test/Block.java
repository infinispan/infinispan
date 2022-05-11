package org.infinispan.query.test;

import java.io.Serializable;

import org.hibernate.search.mapper.pojo.automaticindexing.ReindexOnUpdate;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.IndexingDependency;
import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "blockIndex")
public class Block implements Serializable {

   @Basic
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

   @Embedded
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
