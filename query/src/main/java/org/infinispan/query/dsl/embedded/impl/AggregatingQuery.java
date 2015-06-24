package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.objectfilter.impl.aggregation.Grouper;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
final class AggregatingQuery extends HybridQuery {

   private final int[] groupFieldPositions;

   private final FieldAccumulator[] accumulators;

   AggregatingQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String jpaQuery,
                    int[] groupFieldPositions, FieldAccumulator[] accumulators,
                    ObjectFilter objectFilter,
                    long startOffset, int maxResults,
                    Query baseQuery) {
      super(queryFactory, cache, jpaQuery, objectFilter, startOffset, maxResults, baseQuery);
      this.groupFieldPositions = groupFieldPositions;
      this.accumulators = accumulators;
   }

   @Override
   protected Iterator<?> getBaseIterator() {
      List<Object[]> list = baseQuery.list();
      Grouper grouper = new Grouper(groupFieldPositions, accumulators);
      for (Object[] row : list) {
         grouper.addRow(row);
      }
      return grouper.getGroupIterator();
   }

   @Override
   public String toString() {
      return "AggregatingQuery{" +
            "jpaQuery=" + jpaQuery +
            ", groupFieldPositions=" + Arrays.toString(groupFieldPositions) +
            ", accumulators=" + Arrays.toString(accumulators) +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", baseQuery=" + baseQuery +
            '}';
   }
}
