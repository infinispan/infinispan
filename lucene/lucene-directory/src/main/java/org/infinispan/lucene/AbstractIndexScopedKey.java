package org.infinispan.lucene;

import org.infinispan.protostream.annotations.ProtoField;

abstract class AbstractIndexScopedKey implements IndexScopedKey {

   protected final String indexName;

   protected final int affinitySegmentId;

   protected AbstractIndexScopedKey(String indexName, int affinitySegmentId) {
      this.indexName = indexName;
      this.affinitySegmentId = affinitySegmentId;
   }

   @ProtoField(number = 1)
   @Override
   public String getIndexName() {
      return indexName;
   }

   @ProtoField(number = 2, defaultValue = "-1")
   @Override
   public int getAffinitySegmentId() {
      return affinitySegmentId;
   }
}
