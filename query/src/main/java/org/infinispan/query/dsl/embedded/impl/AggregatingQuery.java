package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.aggregation.Grouper;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
final class AggregatingQuery extends HybridQuery {

   /**
    * The number of columns at the beginning of the row that are used as group key.
    */
   private final int noOfGroupingColumns;

   private final FieldAccumulator[] accumulators;

   private final boolean twoPhaseAcc;

   AggregatingQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String jpaQuery, Map<String, Object> namedParameters,
                    int noOfGroupingColumns, List<FieldAccumulator> accumulators, boolean twoPhaseAcc,
                    ObjectFilter objectFilter,
                    long startOffset, int maxResults,
                    BaseQuery baseQuery) {
      super(queryFactory, cache, jpaQuery, namedParameters, objectFilter, startOffset, maxResults, baseQuery);
      if (baseQuery.getProjection() == null) {
         throw new IllegalArgumentException("Base query must use projections");
      }
      if (projection == null) {
         throw new IllegalArgumentException("Aggregating query must use projections");
      }
      this.noOfGroupingColumns = noOfGroupingColumns;
      this.accumulators = accumulators != null ? accumulators.toArray(new FieldAccumulator[accumulators.size()]) : null;
      this.twoPhaseAcc = twoPhaseAcc;
   }

   @Override
   protected Iterator<?> getBaseIterator() {
      Grouper grouper = new Grouper(noOfGroupingColumns, accumulators, twoPhaseAcc);
      List<Object[]> list = baseQuery.list();
      for (Object[] row : list) {
         grouper.addRow(row);
      }
      return grouper.finish();
   }

   @Override
   public String toString() {
      return "AggregatingQuery{" +
            "jpaQuery=" + jpaQuery +
            ", namedParameters=" + namedParameters +
            ", noOfGroupingColumns=" + noOfGroupingColumns +
            ", accumulators=" + Arrays.toString(accumulators) +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", baseQuery=" + baseQuery +
            '}';
   }
}
