package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

/**
 * A non-indexed query performed on top of the results returned by another query (usually a Lucene based query). This
 * mechanism is used to implement hybrid two-stage queries that perform an index query using a partial query using only
 * the indexed fields and then filter the result again in memory with the full filter.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public class HybridQuery<T, S> extends BaseEmbeddedQuery<T> {

   // An object filter is used to further filter the baseQuery
   protected final ObjectFilter objectFilter;

   protected final Query<S> baseQuery;

   public HybridQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString,
                      Map<String, Object> namedParameters, ObjectFilter objectFilter, long startOffset, int maxResults,
                      Query<?> baseQuery, LocalQueryStatistics queryStatistics, boolean local) {
      super(queryFactory, cache, queryString, namedParameters, objectFilter.getProjection(), startOffset, maxResults, queryStatistics, local);
      this.objectFilter = objectFilter;
      this.baseQuery = (Query<S>) baseQuery;
   }

   @Override
   protected void recordQuery(Long time) {
      queryStatistics.hybridQueryExecuted(queryString, time);
   }

   @Override
   protected Comparator<Comparable<?>[]> getComparator() {
      return objectFilter.getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getInternalIterator() {
      return new MappingIterator<>(getBaseIterator(), objectFilter::filter);
   }

   protected CloseableIterator<?> getBaseIterator() {
      return baseQuery.startOffset(0).maxResults(-1).local(local).iterator();
   }

   @Override
   public String toString() {
      return "HybridQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            ", baseQuery=" + baseQuery +
            '}';
   }
}
