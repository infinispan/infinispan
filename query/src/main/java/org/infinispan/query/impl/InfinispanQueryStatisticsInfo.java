package org.infinispan.query.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.query.Indexer;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;

/**
 * This MBean exposes the query statistics from the Hibernate Search's SearchIntegrator Statistics object via
 * delegation. The Statistics object is transient during search factory in-flight reconfiguration so the instance
 * returned by getStatistics() cannot be registered directly as an MBean.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
@MBean(objectName = "Statistics", description = "Statistics for index based query")
public final class InfinispanQueryStatisticsInfo implements JsonSerialization {

   private final SearchMappingHolder searchMapping;
   private final Indexer massIndexer;
   private final QueryStatistics queryStatistics = new QueryStatistics();
   private final IndexStatistics indexStatistics = new IndexStatistics();

   InfinispanQueryStatisticsInfo(SearchMappingHolder searchMapping, Indexer massIndexer) {
      this.searchMapping = searchMapping;
      this.massIndexer = massIndexer;
   }

   @ManagedOperation
   public void clear() {
      // TODO HSEARCH-3129 Restore support for statistics
      // searchIntegrator.getStatistics().clear();
   }

   @ManagedAttribute
   public long getSearchQueryExecutionCount() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getSearchQueryExecutionCount();
      return 0L;
   }

   @ManagedAttribute
   public long getSearchQueryTotalTime() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getSearchQueryTotalTime();
      return 0L;
   }

   @ManagedAttribute
   public long getSearchQueryExecutionMaxTime() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getSearchQueryExecutionMaxTime();
      return 0L;
   }

   @ManagedAttribute
   public long getSearchQueryExecutionAvgTime() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getSearchQueryExecutionAvgTime();
      return 0L;
   }

   @ManagedAttribute
   public String getSearchQueryExecutionMaxTimeQueryString() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getSearchQueryExecutionMaxTimeQueryString();
      return "";
   }

   @ManagedAttribute
   public long getObjectLoadingTotalTime() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getObjectLoadingTotalTime();
      return 0L;
   }

   @ManagedAttribute
   public long getObjectLoadingExecutionMaxTime() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getObjectLoadingExecutionMaxTime();
      return 0L;
   }

   @ManagedAttribute
   public long getObjectLoadingExecutionAvgTime() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getObjectLoadingExecutionAvgTime();
      return 0L;
   }

   @ManagedAttribute
   public long getObjectsLoadedCount() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getObjectsLoadedCount();
      return 0L;
   }

   @ManagedAttribute(writable = true)
   public boolean isStatisticsEnabled() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().isStatisticsEnabled();
      return false;
   }

   public void setStatisticsEnabled(boolean isStatisticsEnabled) {
      // TODO HSEARCH-3129 Restore support for statistics
      // searchIntegrator.getStatistics().setStatisticsEnabled(isStatisticsEnabled);
   }

   @ManagedAttribute
   public String getSearchVersion() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getSearchVersion();
      return "";
   }

   @ManagedAttribute
   public Set<String> getIndexedClassNames() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getIndexedClassNames();
      return Collections.emptySet();
   }

   @ManagedOperation
   public int getNumberOfIndexedEntities(String entity) {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getNumberOfIndexedEntities(entity);
      return 0;
   }

   @ManagedOperation
   public Map<String, Integer> indexedEntitiesCount() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().indexedEntitiesCount();
      return Collections.emptyMap();
   }

   @ManagedOperation
   public long getIndexSize(String indexName) {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().getIndexSize(indexName);
      return 0L;
   }

   @ManagedOperation
   public Map<String, Long> indexSizes() {
      // TODO HSEARCH-3129 Restore support for statistics
      // return searchIntegrator.getStatistics().indexSizes();
      return Collections.emptyMap();
   }

   public QueryStatistics getQueryStatistics() {
      return queryStatistics;
   }

   public IndexStatistics getIndexStatistics() {
      return indexStatistics;
   }

   @Override
   public Json toJson() {
      return null;
   }

   public final class IndexStatistics implements JsonSerialization {

      public Set<String> getIndexedClassNames() {
         return InfinispanQueryStatisticsInfo.this.getIndexedClassNames();
      }

      public int getNumberOfIndexedEntities(String entity) {
         return InfinispanQueryStatisticsInfo.this.getNumberOfIndexedEntities(entity);
      }

      public Map<String, Integer> getIndexedEntitiesCount() {
         return InfinispanQueryStatisticsInfo.this.indexedEntitiesCount();
      }

      public Map<String, Long> getIndexSizes() {
         return InfinispanQueryStatisticsInfo.this.indexSizes();
      }

      public boolean getReindexing() {
         return massIndexer.isRunning();
      }

      @Override
      public Json toJson() {
         return Json.object()
               .set("indexed_class_names", Json.make(getIndexedClassNames()))
               .set("indexed_entities_count", Json.make(getIndexedEntitiesCount()))
               .set("index_sizes", Json.make(getIndexSizes()))
               .set("reindexing", getReindexing());
      }
   }

   public final class QueryStatistics implements JsonSerialization {

      public long getSearchQueryExecutionCount() {
         return InfinispanQueryStatisticsInfo.this.getSearchQueryExecutionCount();
      }

      public long getSearchQueryTotalTime() {
         return InfinispanQueryStatisticsInfo.this.getSearchQueryTotalTime();
      }

      public long getSearchQueryExecutionMaxTime() {
         return InfinispanQueryStatisticsInfo.this.getSearchQueryExecutionMaxTime();
      }

      public long getSearchQueryExecutionAvgTime() {
         return InfinispanQueryStatisticsInfo.this.getSearchQueryExecutionAvgTime();
      }

      public String getSearchQueryExecutionMaxTimeQueryString() {
         return InfinispanQueryStatisticsInfo.this.getSearchQueryExecutionMaxTimeQueryString();
      }

      public long getObjectLoadingTotalTime() {
         return InfinispanQueryStatisticsInfo.this.getObjectLoadingTotalTime();
      }

      public long getObjectLoadingExecutionMaxTime() {
         return InfinispanQueryStatisticsInfo.this.getObjectLoadingExecutionMaxTime();
      }

      public long getObjectLoadingExecutionAvgTime() {
         return InfinispanQueryStatisticsInfo.this.getObjectLoadingExecutionAvgTime();
      }

      public long getObjectsLoadedCount() {
         return InfinispanQueryStatisticsInfo.this.getObjectsLoadedCount();
      }

      @Override
      public Json toJson() {
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
   }
}
