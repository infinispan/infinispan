package org.infinispan.query.core.stats.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.stats.IndexInfo;

/**
 * Workaround to marshall a Map&lt;String,IndexInfo&gt;.
 *
 * @since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.INDEX_INFO_ENTRY)
class IndexEntry {
   private final String name;
   private final IndexInfo indexInfo;

   @ProtoFactory
   public IndexEntry(String name, IndexInfo indexInfo) {
      this.name = name;
      this.indexInfo = indexInfo;
   }

   @ProtoField(number = 1)
   public String getName() {
      return name;
   }


   @ProtoField(number = 2)
   public IndexInfo getIndexInfo() {
      return indexInfo;
   }
}
