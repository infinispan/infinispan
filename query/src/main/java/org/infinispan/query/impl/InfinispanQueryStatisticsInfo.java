package org.infinispan.query.impl;

import org.hibernate.search.spi.SearchIntegrator;

import java.util.Map;
import java.util.Set;

/**
 * This MBean exposes the query statistics from the Hibernate Search statistics object.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
public class InfinispanQueryStatisticsInfo implements InfinispanQueryStatisticsInfoMBean {

   private final SearchIntegrator sf;

   public InfinispanQueryStatisticsInfo(SearchIntegrator sf) {
      this.sf = sf;
   }

   @Override
   public void clear() {
      sf.getStatistics().clear();
   }

   @Override
   public long getSearchQueryExecutionCount() {
      return sf.getStatistics().getSearchQueryExecutionCount();
   }

   @Override
   public long getSearchQueryTotalTime() {
      return sf.getStatistics().getSearchQueryTotalTime();
   }

   @Override
   public long getSearchQueryExecutionMaxTime() {
      return sf.getStatistics().getSearchQueryExecutionMaxTime();
   }

   @Override
   public long getSearchQueryExecutionAvgTime() {
      return sf.getStatistics().getSearchQueryExecutionAvgTime();
   }

   @Override
   public String getSearchQueryExecutionMaxTimeQueryString() {
      return sf.getStatistics().getSearchQueryExecutionMaxTimeQueryString();
   }

   @Override
   public long getObjectLoadingTotalTime() {
      return sf.getStatistics().getObjectLoadingTotalTime();
   }

   @Override
   public long getObjectLoadingExecutionMaxTime() {
      return sf.getStatistics().getObjectLoadingExecutionMaxTime();
   }

   @Override
   public long getObjectLoadingExecutionAvgTime() {
      return sf.getStatistics().getObjectLoadingExecutionAvgTime();
   }

   @Override
   public long getObjectsLoadedCount() {
      return sf.getStatistics().getObjectsLoadedCount();
   }

   @Override
   public boolean isStatisticsEnabled() {
      return sf.getStatistics().isStatisticsEnabled();
   }

   @Override
   public void setStatisticsEnabled(boolean isStatisticsEnabled) {
      sf.getStatistics().setStatisticsEnabled(isStatisticsEnabled);
   }

   @Override
   public String getSearchVersion() {
      return sf.getStatistics().getSearchVersion();
   }

   @Override
   public Set<String> getIndexedClassNames() {
      return sf.getStatistics().getIndexedClassNames();
   }

   @Override
   public int getNumberOfIndexedEntities(String entity) {
      return sf.getStatistics().getNumberOfIndexedEntities(entity);
   }

   @Override
   public Map<String, Integer> indexedEntitiesCount() {
      return sf.getStatistics().indexedEntitiesCount();
   }
}
