package org.infinispan.query.impl;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.infinispan.security.impl.AuthorizationHelper;

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
   private final AuthorizationHelper authorizationHelper;

   InfinispanQueryStatisticsInfo(SearchStatistics searchStatistics, AuthorizationHelper authorizationHelper) {
      this.queryStatistics = searchStatistics.getQueryStatistics();
      this.indexStatistics = searchStatistics.getIndexStatistics();
      this.authorizationHelper = authorizationHelper;
   }

   @ManagedOperation
   public void clear() {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      queryStatistics.clear();
   }

   @ManagedAttribute
   public long getSearchQueryExecutionCount() {
      return queryStatistics.getLocalIndexedQueryCount();
   }

   @ManagedAttribute
   public long getSearchQueryTotalTime() {
      return queryStatistics.getLocalIndexedQueryTotalTime();
   }

   @ManagedAttribute
   public long getSearchQueryExecutionMaxTime() {
      return queryStatistics.getLocalIndexedQueryMaxTime();
   }

   @ManagedAttribute
   public long getSearchQueryExecutionAvgTime() {
      return ((Double) queryStatistics.getLocalIndexedQueryAvgTime()).longValue();
   }

   @ManagedAttribute
   public String getSearchQueryExecutionMaxTimeQueryString() {
      String query = queryStatistics.getSlowestLocalIndexedQuery();
      return query == null ? "" : query;
   }

   @ManagedAttribute
   public long getObjectLoadingTotalTime() {
      return queryStatistics.getLoadCount();
   }

   @ManagedAttribute
   public long getObjectLoadingExecutionMaxTime() {
      return queryStatistics.getLoadTotalTime();
   }

   @ManagedAttribute
   public long getObjectLoadingExecutionAvgTime() {
      return ((Double) queryStatistics.getLoadAvgTime()).longValue();
   }

   @ManagedAttribute
   public long getObjectsLoadedCount() {
      return queryStatistics.getLoadCount();
   }

   @ManagedAttribute
   public boolean isStatisticsEnabled() {
      return queryStatistics.isEnabled();
   }

   @ManagedAttribute
   public String getSearchVersion() {
      return Version.versionString();
   }

   @ManagedAttribute
   public Set<String> getIndexedClassNames() {
      return indexStatistics.indexInfos().keySet();
   }

   @ManagedOperation
   public int getNumberOfIndexedEntities(String entity) {
      return find(indexStatistics.indexInfos(), entity).map(IndexInfo::count).orElse(0L).intValue();
   }

   private Optional<IndexInfo> find(Map<String, IndexInfo> indexInfos, String entity) {
      return indexInfos.entrySet().stream().filter(e -> e.getKey().equals(entity))
            .map(Map.Entry::getValue).findFirst();
   }

   @ManagedOperation
   public Map<String, Integer> indexedEntitiesCount() {
      return indexStatistics.indexInfos().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> (int) e.getValue().count()));
   }

   @ManagedOperation
   public long getIndexSize(String indexName) {
      return find(indexStatistics.indexInfos(), indexName).map(IndexInfo::size).orElse(0L);
   }

   @ManagedOperation
   public Map<String, Long> indexSizes() {
      return indexStatistics.indexInfos().entrySet().stream()
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

   public Json getLegacyIndexStatistics() {
      Map<String, IndexInfo> indexStats = indexStatistics.indexInfos();
      Map<String, Long> sizes = indexStats.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
      return Json.object()
            .set("indexed_class_names", Json.make(getIndexedClassNames()))
            .set("indexed_entities_count", Json.make(indexStats.keySet()))
            .set("index_sizes", Json.make(sizes))
            .set("reindexing", indexStatistics.reindexing());
   }

}
