package org.infinispan.query.impl;

import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.commons.util.concurrent.jdk8backported.LongAdder;

import java.util.Map;
import java.util.Set;

/**
 * This MBean accumulates query statistics from the Hibernate Search statistics object. The only difference is this
 * statistics continue to accumulate and are not reset when the Search Factory is reconfigured. They are still reset if
 * {@code clear()} is explicitly called though.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
public class InfinispanQueryStatisticsInfo implements InfinispanQueryStatisticsInfoMBean {

   private final SearchFactoryIntegrator sf;
   private final LongAdder searchQueryExecutionCount = new LongAdder();
   private final LongAdder searchQueryTotalTime = new LongAdder();
   private volatile long searchQueryExecutionMaxTime = 0;
   private volatile String searchQueryExecutionMaxTimeQueryString = null;
   private final LongAdder objectLoadingTotalTime = new LongAdder();
   private volatile long objectLoadingExecutionMaxTime = 0;
   private final LongAdder objectLoadedCount = new LongAdder();

   public InfinispanQueryStatisticsInfo(SearchFactoryIntegrator sf) {
      this.sf = sf;
   }

   @Override
   public void clear() {
      searchQueryExecutionCount.reset();
      searchQueryTotalTime.reset();
      searchQueryExecutionMaxTime = 0;
      searchQueryExecutionMaxTimeQueryString = null;
      objectLoadingTotalTime.reset();
      objectLoadingExecutionMaxTime = 0;
      objectLoadedCount.reset();
      sf.getStatistics().clear();
   }

   @Override
   public long getSearchQueryExecutionCount() {
      searchQueryExecutionCount.add(sf.getStatistics().getSearchQueryExecutionCount());
      return searchQueryExecutionCount.sum();
   }

   @Override
   public long getSearchQueryTotalTime() {
      searchQueryTotalTime.add(sf.getStatistics().getSearchQueryTotalTime());
      return searchQueryTotalTime.sum();
   }

   @Override
   public long getSearchQueryExecutionMaxTime() {
      long temp = sf.getStatistics().getSearchQueryExecutionMaxTime();
      if (searchQueryExecutionMaxTime < temp) {
         searchQueryExecutionMaxTime = temp;
      }
      return searchQueryExecutionMaxTime;
   }

   @Override
   public long getSearchQueryExecutionAvgTime() {
      long count = getSearchQueryExecutionCount();
      if (count == 0) {
         return 0;
      }
      return getSearchQueryTotalTime() / count;
   }

   @Override
   public String getSearchQueryExecutionMaxTimeQueryString() {
      String temp = sf.getStatistics().getSearchQueryExecutionMaxTimeQueryString();
      if (temp != null) {
         searchQueryExecutionMaxTimeQueryString = temp;
      }
      return searchQueryExecutionMaxTimeQueryString;
   }

   @Override
   public long getObjectLoadingTotalTime() {
      objectLoadingTotalTime.add(sf.getStatistics().getObjectLoadingTotalTime());
      return objectLoadingTotalTime.sum();
   }

   @Override
   public long getObjectLoadingExecutionMaxTime() {
      long temp = sf.getStatistics().getObjectLoadingExecutionMaxTime();
      if (objectLoadingExecutionMaxTime < temp) {
         objectLoadingExecutionMaxTime = temp;
      }
      return objectLoadingExecutionMaxTime;
   }

   @Override
   public long getObjectLoadingExecutionAvgTime() {
      long count = getObjectsLoadedCount();
      if (count == 0) {
         return 0;
      }
      return getObjectLoadingTotalTime() / count;
   }

   @Override
   public long getObjectsLoadedCount() {
      objectLoadedCount.add(sf.getStatistics().getObjectsLoadedCount());
      return objectLoadedCount.sum();
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
