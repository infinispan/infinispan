package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.TotalHits;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoEnumValue;

@ProtoAdapter(TotalHits.Relation.class)
public enum LuceneTotalHitsRelationAdapter {
   @ProtoEnumValue(1)
   EQUAL_TO,
   @ProtoEnumValue(2)
   GREATER_THAN_OR_EQUAL_TO
}
