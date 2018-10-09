package org.infinispan.query.impl;

import java.util.Map;
import java.util.Set;

import javax.management.ObjectName;

import org.hibernate.search.spi.SearchIntegrator;

/**
 * This MBean exposes the query statistics from the Hibernate Search's SearchIntegrator Statistics object via
 * delegation. The Statistics object is transient during search factory in-flight reconfiguration so the instance
 * returned by getStatistics() cannot be registered directly as an MBean.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
public class InfinispanQueryStatisticsInfo implements InfinispanQueryStatisticsInfoMBean {

   private final SearchIntegrator searchIntegrator;

   private final ObjectName objectName;

   public InfinispanQueryStatisticsInfo(SearchIntegrator searchIntegrator, ObjectName objectName) {
      this.searchIntegrator = searchIntegrator;
      this.objectName = objectName;
   }

   public ObjectName getObjectName() {
      return objectName;
   }

   @Override
   public void clear() {
      searchIntegrator.getStatistics().clear();
   }

   @Override
   public long getSearchQueryExecutionCount() {
      return searchIntegrator.getStatistics().getSearchQueryExecutionCount();
   }

   @Override
   public long getSearchQueryTotalTime() {
      return searchIntegrator.getStatistics().getSearchQueryTotalTime();
   }

   @Override
   public long getSearchQueryExecutionMaxTime() {
      return searchIntegrator.getStatistics().getSearchQueryExecutionMaxTime();
   }

   @Override
   public long getSearchQueryExecutionAvgTime() {
      return searchIntegrator.getStatistics().getSearchQueryExecutionAvgTime();
   }

   @Override
   public String getSearchQueryExecutionMaxTimeQueryString() {
      return searchIntegrator.getStatistics().getSearchQueryExecutionMaxTimeQueryString();
   }

   @Override
   public long getObjectLoadingTotalTime() {
      return searchIntegrator.getStatistics().getObjectLoadingTotalTime();
   }

   @Override
   public long getObjectLoadingExecutionMaxTime() {
      return searchIntegrator.getStatistics().getObjectLoadingExecutionMaxTime();
   }

   @Override
   public long getObjectLoadingExecutionAvgTime() {
      return searchIntegrator.getStatistics().getObjectLoadingExecutionAvgTime();
   }

   @Override
   public long getObjectsLoadedCount() {
      return searchIntegrator.getStatistics().getObjectsLoadedCount();
   }

   @Override
   public boolean isStatisticsEnabled() {
      return searchIntegrator.getStatistics().isStatisticsEnabled();
   }

   @Override
   public void setStatisticsEnabled(boolean isStatisticsEnabled) {
      searchIntegrator.getStatistics().setStatisticsEnabled(isStatisticsEnabled);
   }

   @Override
   public String getSearchVersion() {
      return searchIntegrator.getStatistics().getSearchVersion();
   }

   @Override
   public Set<String> getIndexedClassNames() {
      return searchIntegrator.getStatistics().getIndexedClassNames();
   }

   @Override
   public int getNumberOfIndexedEntities(String entity) {
      return searchIntegrator.getStatistics().getNumberOfIndexedEntities(entity);
   }

   @Override
   public Map<String, Integer> indexedEntitiesCount() {
      return searchIntegrator.getStatistics().indexedEntitiesCount();
   }

   @Override
   public long getIndexSize(String indexName) {
      return searchIntegrator.getStatistics().getIndexSize(indexName);
   }

   @Override
   public Map<String, Long> indexSizes() {
      return searchIntegrator.getStatistics().indexSizes();
   }
}
