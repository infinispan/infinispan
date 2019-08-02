package org.infinispan.lucene;

import org.infinispan.protostream.annotations.ProtoField;

abstract class AbstractIndexScopedKey implements IndexScopedKey {

   @ProtoField(number = 1, name = "index")
   String indexName;

   @ProtoField(number = 2, name = "segment_id", defaultValue = "0")
   int affinitySegmentId;

   AbstractIndexScopedKey() {}

   AbstractIndexScopedKey(String indexName, int affinitySegmentId) {
      this.indexName = indexName;
      this.affinitySegmentId = affinitySegmentId;
   }

   @Override
   public String getIndexName() {
      return indexName;
   }

   @Override
   public int getAffinitySegmentId() {
      return affinitySegmentId;
   }
}
