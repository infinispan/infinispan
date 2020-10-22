package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryFactory;

/**
 * A query that does not return any results because the query filter is a boolean contradiction.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class EmptyResultQuery<T> extends BaseEmbeddedQuery<T> {

   public EmptyResultQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString,
                           Map<String, Object> namedParameters, long startOffset, int maxResults,
                           LocalQueryStatistics queryStatistics) {
      super(queryFactory, cache, queryString, namedParameters, null, startOffset, maxResults, queryStatistics);
   }

   @Override
   protected void recordQuery(Long time) {
   }

   @Override
   protected Comparator<Comparable<?>[]> getComparator() {
      return null;
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getInternalIterator() {
      return new CloseableIterator<ObjectFilter.FilterResult>() {

         @Override
         public void close() {
         }

         @Override
         public boolean hasNext() {
            return false;
         }

         @Override
         public ObjectFilter.FilterResult next() {
            throw new NoSuchElementException();
         }
      };
   }

   @Override
   public String toString() {
      return "EmptyResultQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            '}';
   }
}
