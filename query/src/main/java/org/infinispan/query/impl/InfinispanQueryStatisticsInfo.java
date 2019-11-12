package org.infinispan.query.impl;

import java.util.Map;
import java.util.Set;

import org.hibernate.search.jmx.StatisticsInfoMBean;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.query.MassIndexer;

/**
 * This MBean exposes the query statistics from the Hibernate Search's SearchIntegrator Statistics object via
 * delegation. The Statistics object is transient during search factory in-flight reconfiguration so the instance
 * returned by getStatistics() cannot be registered directly as an MBean.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
@MBean(objectName = "Statistics")
public final class InfinispanQueryStatisticsInfo implements StatisticsInfoMBean {

   private final SearchIntegrator searchIntegrator;
   private final QueryStatistics queryStatistics;
   private final IndexStatistics indexStatistics;

   InfinispanQueryStatisticsInfo(SearchIntegrator searchIntegrator, MassIndexer massIndexer) {
      this.searchIntegrator = searchIntegrator;
      this.queryStatistics = new QueryStatistics(searchIntegrator);
      this.indexStatistics = new IndexStatistics(searchIntegrator, massIndexer);
   }

   @ManagedOperation
   @Override
   public void clear() {
      searchIntegrator.getStatistics().clear();
   }

   @ManagedAttribute
   @Override
   public long getSearchQueryExecutionCount() {
      return queryStatistics.getSearchQueryExecutionCount();
   }

   @ManagedAttribute
   @Override
   public long getSearchQueryTotalTime() {
      return queryStatistics.getSearchQueryTotalTime();
   }

   @ManagedAttribute
   @Override
   public long getSearchQueryExecutionMaxTime() {
      return queryStatistics.getSearchQueryExecutionMaxTime();
   }

   @ManagedAttribute
   @Override
   public long getSearchQueryExecutionAvgTime() {
      return queryStatistics.getSearchQueryExecutionAvgTime();
   }

   @ManagedAttribute
   @Override
   public String getSearchQueryExecutionMaxTimeQueryString() {
      return queryStatistics.getSearchQueryExecutionMaxTimeQueryString();
   }

   @ManagedAttribute
   @Override
   public long getObjectLoadingTotalTime() {
      return queryStatistics.getObjectLoadingTotalTime();
   }

   @ManagedAttribute
   @Override
   public long getObjectLoadingExecutionMaxTime() {
      return queryStatistics.getObjectLoadingExecutionMaxTime();
   }

   @ManagedAttribute
   @Override
   public long getObjectLoadingExecutionAvgTime() {
      return queryStatistics.getObjectLoadingExecutionAvgTime();
   }

   @ManagedAttribute
   @Override
   public long getObjectsLoadedCount() {
      return queryStatistics.getObjectsLoadedCount();
   }

   @ManagedAttribute(writable = true)
   @Override
   public boolean isStatisticsEnabled() {
      return searchIntegrator.getStatistics().isStatisticsEnabled();
   }

   @Override
   public void setStatisticsEnabled(boolean isStatisticsEnabled) {
      searchIntegrator.getStatistics().setStatisticsEnabled(isStatisticsEnabled);
   }

   @ManagedAttribute
   @Override
   public String getSearchVersion() {
      return searchIntegrator.getStatistics().getSearchVersion();
   }

   @ManagedAttribute
   @Override
   public Set<String> getIndexedClassNames() {
      return indexStatistics.getIndexedClassNames();
   }

   @ManagedOperation
   @Override
   public int getNumberOfIndexedEntities(String entity) {
      return indexStatistics.getNumberOfIndexedEntities(entity);
   }

   @ManagedOperation
   @Override
   public Map<String, Integer> indexedEntitiesCount() {
      return indexStatistics.getIndexedEntitiesCount();
   }

   @ManagedOperation
   @Override
   public long getIndexSize(String indexName) {
      return searchIntegrator.getStatistics().getIndexSize(indexName);
   }

   @ManagedOperation
   @Override
   public Map<String, Long> indexSizes() {
      return indexStatistics.getIndexSizes();
   }

   public QueryStatistics getQueryStatistics() {
      return queryStatistics;
   }

   public IndexStatistics getIndexStatistics() {
      return indexStatistics;
   }

   public static class IndexStatistics {
      private final SearchIntegrator searchIntegrator;
      private final MassIndexer massIndexer;

      IndexStatistics(SearchIntegrator searchIntegrator, MassIndexer massIndexer) {
         this.searchIntegrator = searchIntegrator;
         this.massIndexer = massIndexer;
      }

      public Set<String> getIndexedClassNames() {
         return searchIntegrator.getStatistics().getIndexedClassNames();
      }

      public int getNumberOfIndexedEntities(String entity) {
         return searchIntegrator.getStatistics().getNumberOfIndexedEntities(entity);
      }

      public Map<String, Integer> getIndexedEntitiesCount() {
         return searchIntegrator.getStatistics().indexedEntitiesCount();
      }

      public Map<String, Long> getIndexSizes() {
         return searchIntegrator.getStatistics().indexSizes();
      }

      public boolean getReindexing() {
         return massIndexer.isRunning();
      }
   }

   public static class QueryStatistics {

      private final SearchIntegrator searchIntegrator;

      public QueryStatistics(SearchIntegrator searchIntegrator) {
         this.searchIntegrator = searchIntegrator;
      }

      public long getSearchQueryExecutionCount() {
         return searchIntegrator.getStatistics().getSearchQueryExecutionCount();
      }

      public long getSearchQueryTotalTime() {
         return searchIntegrator.getStatistics().getSearchQueryTotalTime();
      }

      public long getSearchQueryExecutionMaxTime() {
         return searchIntegrator.getStatistics().getSearchQueryExecutionMaxTime();
      }

      public long getSearchQueryExecutionAvgTime() {
         return searchIntegrator.getStatistics().getSearchQueryExecutionAvgTime();
      }

      public String getSearchQueryExecutionMaxTimeQueryString() {
         return searchIntegrator.getStatistics().getSearchQueryExecutionMaxTimeQueryString();
      }


      public long getObjectLoadingTotalTime() {
         return searchIntegrator.getStatistics().getObjectLoadingTotalTime();
      }


      public long getObjectLoadingExecutionMaxTime() {
         return searchIntegrator.getStatistics().getObjectLoadingExecutionMaxTime();
      }


      public long getObjectLoadingExecutionAvgTime() {
         return searchIntegrator.getStatistics().getObjectLoadingExecutionAvgTime();
      }


      public long getObjectsLoadedCount() {
         return searchIntegrator.getStatistics().getObjectsLoadedCount();
      }
   }

}
