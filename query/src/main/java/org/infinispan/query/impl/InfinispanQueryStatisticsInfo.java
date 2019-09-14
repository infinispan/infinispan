package org.infinispan.query.impl;

import java.util.Map;
import java.util.Set;

import org.hibernate.search.jmx.StatisticsInfoMBean;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

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

   InfinispanQueryStatisticsInfo(SearchIntegrator searchIntegrator) {
      this.searchIntegrator = searchIntegrator;
   }

   @ManagedOperation
   @Override
   public void clear() {
      searchIntegrator.getStatistics().clear();
   }

   @ManagedAttribute
   @Override
   public long getSearchQueryExecutionCount() {
      return searchIntegrator.getStatistics().getSearchQueryExecutionCount();
   }

   @ManagedAttribute
   @Override
   public long getSearchQueryTotalTime() {
      return searchIntegrator.getStatistics().getSearchQueryTotalTime();
   }

   @ManagedAttribute
   @Override
   public long getSearchQueryExecutionMaxTime() {
      return searchIntegrator.getStatistics().getSearchQueryExecutionMaxTime();
   }

   @ManagedAttribute
   @Override
   public long getSearchQueryExecutionAvgTime() {
      return searchIntegrator.getStatistics().getSearchQueryExecutionAvgTime();
   }

   @ManagedAttribute
   @Override
   public String getSearchQueryExecutionMaxTimeQueryString() {
      return searchIntegrator.getStatistics().getSearchQueryExecutionMaxTimeQueryString();
   }

   @ManagedAttribute
   @Override
   public long getObjectLoadingTotalTime() {
      return searchIntegrator.getStatistics().getObjectLoadingTotalTime();
   }

   @ManagedAttribute
   @Override
   public long getObjectLoadingExecutionMaxTime() {
      return searchIntegrator.getStatistics().getObjectLoadingExecutionMaxTime();
   }

   @ManagedAttribute
   @Override
   public long getObjectLoadingExecutionAvgTime() {
      return searchIntegrator.getStatistics().getObjectLoadingExecutionAvgTime();
   }

   @ManagedAttribute
   @Override
   public long getObjectsLoadedCount() {
      return searchIntegrator.getStatistics().getObjectsLoadedCount();
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
      return searchIntegrator.getStatistics().getIndexedClassNames();
   }

   @ManagedOperation
   @Override
   public int getNumberOfIndexedEntities(String entity) {
      return searchIntegrator.getStatistics().getNumberOfIndexedEntities(entity);
   }

   @ManagedOperation
   @Override
   public Map<String, Integer> indexedEntitiesCount() {
      return searchIntegrator.getStatistics().indexedEntitiesCount();
   }

   @ManagedOperation
   @Override
   public long getIndexSize(String indexName) {
      return searchIntegrator.getStatistics().getIndexSize(indexName);
   }

   @ManagedOperation
   @Override
   public Map<String, Long> indexSizes() {
      return searchIntegrator.getStatistics().indexSizes();
   }
}
