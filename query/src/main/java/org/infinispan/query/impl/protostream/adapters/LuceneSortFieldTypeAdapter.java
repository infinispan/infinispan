package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.SortField;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(SortField.Type.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_SORT_FIELD_TYPE)
public enum LuceneSortFieldTypeAdapter {
   @ProtoEnumValue(1)
   SCORE,
   @ProtoEnumValue(2)
   DOC,
   @ProtoEnumValue(3)
   STRING,
   @ProtoEnumValue(4)
   INT,
   @ProtoEnumValue(5)
   FLOAT,
   @ProtoEnumValue(6)
   LONG,
   @ProtoEnumValue(7)
   DOUBLE,
   @ProtoEnumValue(8)
   CUSTOM,
   @ProtoEnumValue(9)
   STRING_VAL,
   @ProtoEnumValue(10)
   REWRITEABLE
}
