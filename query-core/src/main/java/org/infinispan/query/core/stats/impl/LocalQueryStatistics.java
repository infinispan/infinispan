package org.infinispan.query.core.stats.impl;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.stats.QueryStatisticsSnapshot;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.Authorizer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 *  Manages query statistics for a Cache.
 *
 * @since 12.0
 */
@Scope(Scopes.NAMED_CACHE)
@ProtoTypeId(ProtoStreamTypeIds.LOCAL_QUERY_STATS)
public class LocalQueryStatistics implements QueryStatisticsSnapshot {

   @ProtoField(number = 1)
   QueryMetrics localIndexedQueries = new QueryMetrics();

   @ProtoField(number = 2)
   QueryMetrics distIndexedQueries = new QueryMetrics();

   @ProtoField(number = 3)
   QueryMetrics hybridQueries = new QueryMetrics();

   @ProtoField(number = 4)
   QueryMetrics nonIndexedQueries = new QueryMetrics();

   @ProtoField(number = 5)
   QueryMetrics loads = new QueryMetrics();

   @Inject
   Configuration configuration;

   @Inject
   Authorizer authorizer;


   public LocalQueryStatistics() {
   }

   @ProtoFactory
   LocalQueryStatistics(QueryMetrics localIndexedQueries, QueryMetrics distIndexedQueries,
                        QueryMetrics hybridQueries, QueryMetrics nonIndexedQueries,
                        QueryMetrics loads) {
      this.localIndexedQueries = localIndexedQueries;
      this.distIndexedQueries = distIndexedQueries;
      this.hybridQueries = hybridQueries;
      this.nonIndexedQueries = nonIndexedQueries;
      this.loads = loads;
   }

   public void localIndexedQueryExecuted(String q, long timeNanos) {
      localIndexedQueries.record(q, timeNanos);
   }

   public void distributedIndexedQueryExecuted(String q, long timeNanos) {
      distIndexedQueries.record(q, timeNanos);
   }

   public void hybridQueryExecuted(String q, long timeNanos) {
      hybridQueries.record(q, timeNanos);
   }

   public void nonIndexedQueryExecuted(String q, long timeNanos) {
      nonIndexedQueries.record(q, timeNanos);
   }

   public void entityLoaded(long timeNanos) {
      loads.record(timeNanos);
   }

   /**
    * @return Number of queries executed in the local index.
    */
   @Override
   public long getLocalIndexedQueryCount() {
      return localIndexedQueries.count();
   }

   /**
    * @return Number of distributed indexed queries executed from the local node.
    */
   @Override
   public long getDistributedIndexedQueryCount() {
      return distIndexedQueries.count();
   }

   /**
    * @return Number of hybrid queries (two phase indexed and non-indexed) executed from the local node.
    */
   @Override
   public long getHybridQueryCount() {
      return hybridQueries.count();
   }

   /**
    * @return Number of non-indexed queries executed from the local node.
    */
   @Override
   public long getNonIndexedQueryCount() {
      return nonIndexedQueries.count();
   }

   /**
    * @return The total time in nanoseconds of all indexed queries.
    */
   @Override
   public long getLocalIndexedQueryTotalTime() {
      return localIndexedQueries.totalTime();
   }

   /**
    * @return The total time in nanoseconds of all distributed indexed queries.
    */
   @Override
   public long getDistributedIndexedQueryTotalTime() {
      return distIndexedQueries.totalTime();
   }

   /**
    * @return The total time in nanoseconds for all hybrid queries.
    */
   @Override
   public long getHybridQueryTotalTime() {
      return hybridQueries.totalTime();
   }

   /**
    * @return The total time in nanoseconds for all non-indexed queries.
    */
   @Override
   public long getNonIndexedQueryTotalTime() {
      return nonIndexedQueries.totalTime();
   }

   /**
    * @return The time in nanoseconds of the slowest indexed query.
    */
   @Override
   public long getLocalIndexedQueryMaxTime() {
      return localIndexedQueries.maxTime();
   }

   /**
    * @return The time in nanoseconds of the slowest distributed indexed query.
    */
   @Override
   public long getDistributedIndexedQueryMaxTime() {
      return distIndexedQueries.maxTime();
   }

   /**
    * @return The time in nanoseconds of the slowest hybrid query.
    */
   @Override
   public long getHybridQueryMaxTime() {
      return hybridQueries.maxTime();
   }

   /**
    * @return The time in nanoseconds of the slowest non-indexed query.
    */
   @Override
   public long getNonIndexedQueryMaxTime() {
      return nonIndexedQueries.maxTime();
   }

   /**
    * @return The average time in nanoseconds of all indexed queries.
    */
   @Override
   public double getLocalIndexedQueryAvgTime() {
      return localIndexedQueries.avg();
   }

   /**
    * @return The average time in nanoseconds of all distributed indexed queries.
    */
   @Override
   public double getDistributedIndexedQueryAvgTime() {
      return distIndexedQueries.avg();
   }

   /**
    * @return The average time in nanoseconds of all hybrid indexed queries.
    */
   @Override
   public double getHybridQueryAvgTime() {
      return hybridQueries.avg();
   }

   /**
    * @return The average time in nanoseconds of all non-indexed indexed queries.
    */
   @Override
   public double getNonIndexedQueryAvgTime() {
      return nonIndexedQueries.avg();
   }

   /**
    * @return The Ickle query string of the slowest indexed query.
    */
   @Override
   public String getSlowestLocalIndexedQuery() {
      return localIndexedQueries.slowest();
   }

   /**
    * @return The Ickle query string of the slowest distributed indexed query.
    */
   @Override
   public String getSlowestDistributedIndexedQuery() {
      return distIndexedQueries.slowest();
   }

   /**
    * @return The Ickle query string of the slowest hybrid query.
    */
   @Override
   public String getSlowestHybridQuery() {
      return hybridQueries.slowest();
   }

   /**
    * @return The Ickle query string of the slowest non-indexed query.
    */
   @Override
   public String getSlowestNonIndexedQuery() {
      return nonIndexedQueries.slowest();
   }

   /**
    * @return The max time in nanoseconds to load entities from a Cache after an indexed query.
    */
   @Override
   public long getLoadMaxTime() {
      return loads.maxTime();
   }

   /**
    * @return The average time in nanoseconds to load entities from a Cache after an indexed query.
    */
   @Override
   public double getLoadAvgTime() {
      return loads.avg();
   }

   /**
    * @return The number of entities loaded from a Cache after an indexed query.
    */
   @Override
   public long getLoadCount() {
      return loads.count();
   }

   /**
    * @return The total time to load entities from a Cache after an indexed query.
    */
   @Override
   public long getLoadTotalTime() {
      return loads.totalTime();
   }

   protected QueryMetrics getLocalIndexedQueries() {
      return localIndexedQueries;
   }

   protected QueryMetrics getDistIndexedQueries() {
      return distIndexedQueries;
   }

   protected QueryMetrics getHybridQueries() {
      return hybridQueries;
   }

   protected QueryMetrics getNonIndexedQueries() {
      return nonIndexedQueries;
   }

   public QueryMetrics getLoads() {
      return loads;
   }

   @Override
   public boolean isEnabled() {
      return configuration.statistics().enabled();
   }

   @Override
   public CompletionStage<QueryStatisticsSnapshot> computeSnapshot() {
      return CompletableFuture.completedFuture(new LocalQueryStatistics().merge(this));
   }

   @Override
   public void clear() {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      localIndexedQueries.clear();
      distIndexedQueries.clear();
      nonIndexedQueries.clear();
      hybridQueries.clear();
      loads.clear();
   }

   private void mergeMetrics(QueryMetrics metrics, long count, long totalTime, long maxTime, String slowest) {
      metrics.count.add(count);
      metrics.totalTime.add(totalTime);
      if (metrics.maxTime.longValue() < maxTime) {
         metrics.maxTime.set(maxTime);
         metrics.slowest = slowest;
      }
   }

   @Override
   public QueryStatisticsSnapshot merge(QueryStatisticsSnapshot other) {
      mergeMetrics(this.localIndexedQueries, other.getLocalIndexedQueryCount(), other.getLocalIndexedQueryTotalTime(),
            other.getLocalIndexedQueryMaxTime(), other.getSlowestLocalIndexedQuery());
      mergeMetrics(this.distIndexedQueries, other.getDistributedIndexedQueryCount(), other.getDistributedIndexedQueryTotalTime(),
            other.getDistributedIndexedQueryMaxTime(), other.getSlowestDistributedIndexedQuery());
      mergeMetrics(this.hybridQueries, other.getHybridQueryCount(), other.getHybridQueryTotalTime(),
            other.getHybridQueryMaxTime(), other.getSlowestHybridQuery());
      mergeMetrics(this.nonIndexedQueries, other.getNonIndexedQueryCount(), other.getNonIndexedQueryMaxTime(),
            other.getNonIndexedQueryMaxTime(), other.getSlowestNonIndexedQuery());
      mergeMetrics(this.loads, other.getLoadCount(), other.getLoadTotalTime(),
            other.getLoadMaxTime(), null);
      return this;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("indexed_local", Json.make(getLocalIndexedQueries()))
            .set("indexed_distributed", Json.make(getDistIndexedQueries()))
            .set("hybrid", Json.make(getHybridQueries()))
            .set("non_indexed", Json.make(getNonIndexedQueries()))
            .set("entity_load", Json.make(getLoads()));
   }

}
