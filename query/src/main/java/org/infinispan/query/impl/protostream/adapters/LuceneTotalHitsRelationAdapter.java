package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.TotalHits;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoAdapter;

@Proto
@ProtoAdapter(TotalHits.Relation.class)
public enum LuceneTotalHitsRelationAdapter {
   EQUAL_TO,
   GREATER_THAN_OR_EQUAL_TO
}
