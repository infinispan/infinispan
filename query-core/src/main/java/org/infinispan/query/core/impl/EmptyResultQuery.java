package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.util.logging.LogFactory;

/**
 * A query that does not return any results because the query filter is a boolean contradiction.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class EmptyResultQuery<T> extends BaseEmbeddedQuery<T> {

   private static final Log LOG = LogFactory.getLog(EmptyResultQuery.class, Log.class);

   public EmptyResultQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString,
                           IckleParsingResult.StatementType statementType,
                           Map<String, Object> namedParameters, long startOffset, int maxResults,
                           LocalQueryStatistics queryStatistics) {
      super(queryFactory, cache, queryString, statementType, namedParameters, null, startOffset, maxResults, queryStatistics, false);
   }

   @Override
   protected void recordQuery(long time) {
      // this never got executed, we could record 0 but that would just pollute the stats
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
   public int executeStatement() {
      if (isSelectStatement()) {
         throw LOG.unsupportedStatement();
      }
      return 0;
   }

   @Override
   public String toString() {
      return "EmptyResultQuery{" +
            "queryString=" + queryString +
            ", statementType=" + statementType +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            '}';
   }
}
