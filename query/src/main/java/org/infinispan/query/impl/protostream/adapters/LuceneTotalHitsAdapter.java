package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.TotalHits;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(TotalHits.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_TOTAL_HITS)
public class LuceneTotalHitsAdapter {

   @ProtoFactory
   static TotalHits protoFactory(long value, TotalHits.Relation relation) {
      return new TotalHits(value, relation);
   }

   @ProtoField(value = 1, defaultValue = "0")
   long getValue(TotalHits totalHits) {
      return totalHits.value;
   }

   @ProtoField(2)
   TotalHits.Relation getRelation(TotalHits totalHits) {
      return totalHits.relation;
   }
}
