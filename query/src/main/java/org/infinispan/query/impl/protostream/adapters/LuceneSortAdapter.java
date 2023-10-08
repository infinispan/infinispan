package org.infinispan.query.impl.protostream.adapters;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(Sort.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_SORT)
public class LuceneSortAdapter {

   @ProtoFactory
   static Sort protoFactory(SortField[] sortFields) {
      return new Sort(sortFields);
   }

   @ProtoField(1)
   SortField[] getSortFields(Sort sort) {
      return sort.getSort();
   }
}
