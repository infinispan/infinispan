package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.SortField;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoTypeId;

@Proto
@ProtoAdapter(SortField.Type.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_SORT_FIELD_TYPE)
public enum LuceneSortFieldTypeAdapter {
   SCORE,
   DOC,
   STRING,
   INT,
   FLOAT,
   LONG,
   DOUBLE,
   CUSTOM,
   STRING_VAL,
   REWRITEABLE
}
