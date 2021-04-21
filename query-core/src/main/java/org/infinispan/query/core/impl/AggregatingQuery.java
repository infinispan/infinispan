package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.aggregation.RowGrouper;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;

/**
 * Executes grouping and aggregation on top of a base query.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class AggregatingQuery<T> extends HybridQuery<T, Object[]> {

   /**
    * The number of columns at the beginning of the row that are used as group key.
    */
   private final int noOfGroupingColumns;

   private final FieldAccumulator[] accumulators;

   private final boolean twoPhaseAcc;

   public AggregatingQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache,
                           String queryString, Map<String, Object> namedParameters,
                           int noOfGroupingColumns, List<FieldAccumulator> accumulators, boolean twoPhaseAcc,
                           ObjectFilter objectFilter,
                           long startOffset, int maxResults,
                           BaseQuery<?> baseQuery, LocalQueryStatistics queryStatistics) {
      super(queryFactory, cache, queryString, namedParameters, objectFilter, startOffset, maxResults, baseQuery, queryStatistics);
      if (!baseQuery.hasProjections()) {
         throw new IllegalArgumentException("Base query must use projections");
      }
      if (projection == null) {
         throw new IllegalArgumentException("Aggregating query must use projections");
      }
      this.noOfGroupingColumns = noOfGroupingColumns;
      this.accumulators = accumulators != null ? accumulators.toArray(new FieldAccumulator[0]) : null;
      this.twoPhaseAcc = twoPhaseAcc;
   }

   @Override
   protected CloseableIterator<?> getBaseIterator() {
      RowGrouper grouper = new RowGrouper(noOfGroupingColumns, accumulators, twoPhaseAcc);
      try (CloseableIterator<Object[]> iterator = baseQuery.iterator()) {
         iterator.forEachRemaining(item -> grouper.addRow(item));
      }
      return Closeables.iterator(grouper.finish());
   }

   @Override
   public String toString() {
      return "AggregatingQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", noOfGroupingColumns=" + noOfGroupingColumns +
            ", accumulators=" + Arrays.toString(accumulators) +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            ", baseQuery=" + baseQuery +
            '}';
   }
}
