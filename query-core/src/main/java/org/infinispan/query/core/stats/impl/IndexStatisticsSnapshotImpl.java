package org.infinispan.query.core.stats.impl;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.IndexStatisticsSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A snapshot for {@link IndexStatistics}.
 *
 * @since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.LOCAL_INDEX_STATS)
public class IndexStatisticsSnapshotImpl implements IndexStatisticsSnapshot {

   private final Map<String, IndexInfo> indexInfos;
   private final boolean reindexing;
   private int genericIndexingFailures;
   private int entityIndexingFailures;

   public IndexStatisticsSnapshotImpl() {
      // Don't use Collections.emptyMap() here, the map needs to be mutable to support merge().
      this(new HashMap<>(), false, 0, 0);
   }

   @ProtoFactory
   public IndexStatisticsSnapshotImpl(List<IndexEntry> entries, boolean reindexing, int genericIndexingFailures, int entityIndexingFailures) {
      this(toMap(entries), reindexing, genericIndexingFailures, entityIndexingFailures);
   }

   public IndexStatisticsSnapshotImpl(Map<String, IndexInfo> indexInfos, boolean reindexing, int genericIndexingFailures, int entityIndexingFailures) {
      this.indexInfos = indexInfos;
      this.reindexing = reindexing;
      this.genericIndexingFailures = genericIndexingFailures;
      this.entityIndexingFailures = entityIndexingFailures;
   }

   @Override
   public Map<String, IndexInfo> indexInfos() {
      return indexInfos;
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   public List<IndexEntry> getEntries() {
      return fromMap(indexInfos);
   }

   static Map<String, IndexInfo> toMap(List<IndexEntry> entries) {
      return Collections.unmodifiableMap(entries.stream().collect(Collectors.toMap(IndexEntry::getName, IndexEntry::getIndexInfo)));
   }

   static List<IndexEntry> fromMap(Map<String, IndexInfo> map) {
      return map.entrySet().stream()
            .map(e -> new IndexEntry(e.getKey(), e.getValue())).collect(Collectors.toList());
   }

   @Override
   @ProtoField(number = 2, defaultValue = "false")
   public boolean reindexing() {
      return reindexing;
   }

   @Override
   @ProtoField(number = 3, defaultValue = "0")
   public int genericIndexingFailures() {
      return genericIndexingFailures;
   }

   @Override
   @ProtoField(number = 4, defaultValue = "0")
   public int entityIndexingFailures() {
      return entityIndexingFailures;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("types", Json.make(indexInfos))
            .set("reindexing", Json.make(reindexing))
            .set("genericIndexingFailures", Json.make(genericIndexingFailures))
            .set("entityIndexingFailures", Json.make(entityIndexingFailures));
   }

   @Override
   public IndexStatisticsSnapshot merge(IndexStatisticsSnapshot other) {
      other.indexInfos().forEach((k, v) -> indexInfos.merge(k, v, IndexInfo::merge));
      genericIndexingFailures += other.genericIndexingFailures();
      entityIndexingFailures += other.entityIndexingFailures();
      return this;
   }
}
