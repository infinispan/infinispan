package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.util.logging.LogFactory;

/**
 * A non-indexed query performed on top of the results returned by another query (usually a Lucene based query). This
 * mechanism is used to implement hybrid two-stage queries that perform an index query using a partial query using only
 * the indexed fields and then filter the result again in memory with the full filter.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public class HybridQuery<T, S> extends BaseEmbeddedQuery<T> {

   private static final Log LOG = LogFactory.getLog(HybridQuery.class, Log.class);

   // An object filter is used to further filter the baseQuery
   protected final ObjectFilter objectFilter;

   protected final Query<S> baseQuery;

   public HybridQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString, IckleParsingResult.StatementType statementType,
                      Map<String, Object> namedParameters, ObjectFilter objectFilter, long startOffset, int maxResults,
                      Query<?> baseQuery, LocalQueryStatistics queryStatistics, boolean local) {
      super(queryFactory, cache, queryString, statementType, namedParameters, objectFilter.getProjection(), startOffset, maxResults, queryStatistics, local);
      this.objectFilter = objectFilter;
      this.baseQuery = (Query<S>) baseQuery;
   }

   @Override
   protected void recordQuery(long time) {
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
      // Hybrid query, as they are, they require an unbounded max results, another reason to avoid using them
      return baseQuery.startOffset(0).maxResults(Integer.MAX_VALUE).local(local).iterator();
   }

   @Override
   public QueryResult<T> execute() {
      if (isSelectStatement()) {
         return super.execute();
      }

      return new QueryResultImpl<>(executeStatement(), Collections.emptyList());
   }

   @Override
   public int executeStatement() {
      if (isSelectStatement()) {
         throw LOG.unsupportedStatement();
      }

      try (CloseableIterator<Map.Entry<Object, S>> entryIterator = baseQuery.startOffset(0).maxResults(-1).local(local).entryIterator()) {
         Iterator<ObjectFilter.FilterResult> it = new MappingIterator<>(entryIterator, e -> objectFilter.filter(e.getKey(), e.getValue()));
         int count = 0;
         while (it.hasNext()) {
            ObjectFilter.FilterResult fr = it.next();
            Object removed = cache.remove(fr.getKey());
            if (removed != null) {
               count++;
            }
         }
         return count;
      }
   }

   @Override
   public String toString() {
      return "HybridQuery{" +
            "queryString=" + queryString +
            ", statementType=" + statementType +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            ", baseQuery=" + baseQuery +
            '}';
   }
}
