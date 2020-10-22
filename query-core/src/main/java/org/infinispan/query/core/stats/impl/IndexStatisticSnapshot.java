package org.infinispan.query.core.stats.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;

/**
 * A snapshot for {@link IndexStatistics}.
 *
 * @since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.LOCAL_INDEX_STATS)
public class IndexStatisticSnapshot implements IndexStatistics {

   private final Map<String, IndexInfo> indexInfos;

   public IndexStatisticSnapshot(Map<String, IndexInfo> indexInfos) {
      this.indexInfos = indexInfos;
   }

   @ProtoFactory
   public IndexStatisticSnapshot(List<IndexEntry> entries) {
      this.indexInfos = toMap(entries);
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   public List<IndexEntry> getEntries() {
      return fromMap(indexInfos);
   }

   static Map<String, IndexInfo> toMap(List<IndexEntry> entries) {
      return entries.stream().collect(Collectors.toMap(IndexEntry::getName, IndexEntry::getIndexInfo));
   }

   static List<IndexEntry> fromMap(Map<String, IndexInfo> map) {
      return map.entrySet().stream()
            .map(e -> new IndexEntry(e.getKey(), e.getValue())).collect(Collectors.toList());
   }

   @Override
   public Map<String, IndexInfo> indexInfos() {
      return Collections.unmodifiableMap(indexInfos);
   }

   @Override
   public IndexStatistics merge(IndexStatistics other) {
      other.indexInfos().forEach((k, v) -> indexInfos.merge(k, v, IndexInfo::merge));
      return this;
   }

   public IndexStatistics getSnapshot() {
      return this;
   }

   @Override
   public Json toJson() {
      return Json.make(indexInfos());
   }

}
