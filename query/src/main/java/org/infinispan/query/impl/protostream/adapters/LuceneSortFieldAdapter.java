package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.SortField;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(SortField.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_SORT_FIELD)
public class LuceneSortFieldAdapter {
   @ProtoFactory
   static SortField protoFactory(String field, SortField.Type type, boolean reverseSort) {
      return new SortField(field, type, reverseSort);
   }

   @ProtoField(1)
   String getField(SortField field) {
      return field.getField();
   }

   @ProtoField(2)
   SortField.Type getType(SortField field) {
      return field.getType();
   }

   @ProtoField(3)
   boolean getReverseSort(SortField field) {
      return field.getReverse();
   }
}
