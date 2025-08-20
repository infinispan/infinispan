package org.infinispan.query.core.stats;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Runtime information about an index.
 *
 * @since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.INDEX_INFO)
public class IndexInfo implements JsonSerialization {
   private final long count;
   private final long size;

   @ProtoFactory
   public IndexInfo(long count, long size) {
      this.count = count;
      this.size = size;
   }

   /**
    * @return Number of entities indexed.
    */
   @ProtoField(number = 1, defaultValue = "0")
   public long count() {
      return count;
   }

   /**
    * @return Size of index in bytes.
    */
   @ProtoField(number = 2, defaultValue = "0")
   public long size() {
      return size;
   }

   public IndexInfo merge(IndexInfo indexInfo) {
      long mergedCount = count, mergedSize = size;
      mergedCount += indexInfo.count;
      mergedSize += indexInfo.size;
      return new IndexInfo(mergedCount, mergedSize);
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("count", count())
            .set("size", size());
   }

   @Override
   public String toString() {
      return "IndexInfo{" +
            "count=" + count +
            ", size=" + size +
            '}';
   }
}
