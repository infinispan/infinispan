package org.infinispan.query.impl;

import org.hibernate.search.engine.Version;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.commons.util.concurrent.CompletionStages;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * This MBean exposes the query statistics from the Hibernate Search's SearchIntegrator Statistics object via
 * delegation. The Statistics object is transient during search factory in-flight reconfiguration so the instance
 * returned by getStatistics() cannot be registered directly as an MBean.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
@MBean(objectName = "Statistics", description = "Statistics for index based query")
public final class InfinispanQueryStatisticsInfo {

   private final QueryStatistics queryStatistics;
   private final IndexStatistics indexStatistics;
   private final Authorizer authorizer;

   InfinispanQueryStatisticsInfo(SearchStatistics searchStatistics, Authorizer authorizer) {
      this.queryStatistics = searchStatistics.getQueryStatistics();
      this.indexStatistics = searchStatistics.getIndexStatistics();
      this.authorizer = authorizer;
   }

   @ManagedOperation(description = "Clear all query statistics.",
         displayName = "Clear query statistics.")
   public void clear() {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      queryStatistics.clear();
   }

   @ManagedAttribute(description = "Number of queries executed in the local index.",
         displayName = "Local indexed query count.")
   public long getSearchQueryExecutionCount() {
      return queryStatistics.getLocalIndexedQueryCount();
   }

   @ManagedAttribute(description = "The total time in nanoseconds of all indexed queries.",
         displayName = "Local indexed query total time.")
   public long getSearchQueryTotalTime() {
      return queryStatistics.getLocalIndexedQueryTotalTime();
   }

   @ManagedAttribute(description = "The time in nanoseconds of the slowest indexed query.",
         displayName = "Local indexed query max time.")
   public long getSearchQueryExecutionMaxTime() {
      return queryStatistics.getLocalIndexedQueryMaxTime();
   }

   @ManagedAttribute(description = "The average time in nanoseconds of all indexed queries.",
         displayName = "Local indexed query avg time.")
   public long getSearchQueryExecutionAvgTime() {
      return ((Double) queryStatistics.getLocalIndexedQueryAvgTime()).longValue();
   }

   @ManagedAttribute(description = "The Ickle query string of the slowest indexed query.",
         displayName = "Local slowest query.")
   public String getSearchQueryExecutionMaxTimeQueryString() {
      String query = queryStatistics.getSlowestLocalIndexedQuery();
      return query == null ? "" : query;
   }

   @ManagedAttribute(description = "The total time to load entities from a Cache after an indexed query.",
         displayName = "Entity loading total time.")
   public long getObjectLoadingTotalTime() {
      return queryStatistics.getLoadTotalTime();
   }

   @ManagedAttribute(description = "The max time in nanoseconds to load entities from a Cache after an indexed query.",
         displayName = "Entity loading max time.")
   public long getObjectLoadingExecutionMaxTime() {
      return queryStatistics.getLoadMaxTime();
   }

   @ManagedAttribute(description = " The average time in nanoseconds to load entities from a Cache after an indexed query.",
         displayName = "Entity loading avg time.")
   public long getObjectLoadingExecutionAvgTime() {
      return ((Double) queryStatistics.getLoadAvgTime()).longValue();
   }

   @ManagedAttribute(description = "The number of operations to load entities from a Cache after an indexed query.",
         displayName = "Entity loading count.")
   public long getObjectsLoadedCount() {
      return queryStatistics.getLoadCount();
   }

   @ManagedAttribute(description = "True if the Cache has statistics enabled.",
         displayName = "Statistics enabled.")
   public boolean isStatisticsEnabled() {
      return queryStatistics.isEnabled();
   }

   @ManagedAttribute(description = "The Hibernate Search version used as query engine for the cache.",
         displayName = "Hibernate Search version.")
   public String getSearchVersion() {
      return Version.versionString();
   }

   @ManagedAttribute(description = "The name of all indexed entities configured in the cache. The name of the entity " +
         "is either the class name annotated with @Index, or the protobuf Message name.",
         displayName = "Indexed entity names")
   public Set<String> getIndexedClassNames() {
      return indexStatistics.indexedEntities();
   }

   @ManagedOperation(description = "The number of indexed entities for a given entity.",
         displayName = "Indexed entries by entity.")
   public int getNumberOfIndexedEntities(String entity) {
      return find(blockingIndexInfos(), entity).map(IndexInfo::count).orElse(0L).intValue();
   }

   private Optional<IndexInfo> find(Map<String, IndexInfo> indexInfos, String entity) {
      return indexInfos.entrySet().stream().filter(e -> e.getKey().equals(entity))
            .map(Map.Entry::getValue).findFirst();
   }

   @ManagedOperation(description = "The number of indexed entities for all entities.",
         displayName = "Indexed entries by entities.")
   public Map<String, Integer> indexedEntitiesCount() {
      return blockingIndexInfos().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (int) e.getValue().count()));
   }

   @ManagedOperation(description = "The index size for a given index name.",
         displayName = "Index size by name.")
   public long getIndexSize(String indexName) {
      return find(blockingIndexInfos(), indexName).map(IndexInfo::size).orElse(0L);
   }

   @ManagedOperation(description = "All index sizes by their names.",
         displayName = "Index sizes by names.")
   public Map<String, Long> indexSizes() {
      return blockingIndexInfos().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
   }

   public Json getLegacyQueryStatistics() {
      return Json.object()
            .set("search_query_execution_count", getSearchQueryExecutionCount())
            .set("search_query_total_time", getSearchQueryTotalTime())
            .set("search_query_execution_max_time", getSearchQueryExecutionMaxTime())
            .set("search_query_execution_avg_time", getSearchQueryExecutionAvgTime())
            .set("object_loading_total_time", getObjectLoadingTotalTime())
            .set("object_loading_execution_max_time", getObjectLoadingExecutionMaxTime())
            .set("object_loading_execution_avg_time", getObjectLoadingExecutionAvgTime())
            .set("objects_loaded_count", getObjectsLoadedCount())
            .set("search_query_execution_max_time_query_string", getSearchQueryExecutionMaxTimeQueryString());
   }

   public CompletionStage<Json> computeLegacyIndexStatistics() {
      return indexStatistics.computeIndexInfos().thenApply(indexInfos -> {
         Map<String, Long> counts = indexInfos.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().count()));
         Map<String, Long> sizes = indexInfos.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
         return Json.object()
               .set("indexed_class_names", Json.make(indexInfos.keySet()))
               .set("indexed_entities_count", Json.make(counts))
               .set("index_sizes", Json.make(sizes))
               .set("reindexing", indexStatistics.reindexing());
      });
   }

   private Map<String, IndexInfo> blockingIndexInfos() {
      return CompletionStages.join(indexStatistics.computeIndexInfos());
   }

}
