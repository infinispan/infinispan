package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.objectfilter.ObjectFilter;
import org.infinispan.query.objectfilter.impl.syntax.parser.IckleParsingResult;
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

   private final boolean allSortFieldsAreStored;

   public HybridQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString, IckleParsingResult.StatementType statementType,
                      Map<String, Object> namedParameters, ObjectFilter objectFilter, long startOffset, int maxResults,
                      Query<?> baseQuery, LocalQueryStatistics queryStatistics, boolean local, boolean allSortFieldsAreStored) {
      super(queryFactory, cache, queryString, statementType, namedParameters, objectFilter.getProjection(), startOffset, maxResults, queryStatistics, local);
      this.objectFilter = objectFilter;
      this.baseQuery = (Query<S>) baseQuery;
      this.allSortFieldsAreStored = allSortFieldsAreStored;
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
   protected ClosableIteratorWithCount<ObjectFilter.FilterResult> getInternalIterator() {
      return new MappingIterator<>(getBaseIterator(), objectFilter::filter);
   }

   protected CloseableIterator<?> getBaseIterator() {
      return baseQuery.startOffset(0).maxResults(hybridMaxResult()).local(local).iterator();
   }

   protected int hybridMaxResult() {
      // Hybrid query, if all sort fields are not index-sortable, they require an unbounded max results, another reason to avoid using them.
      // The other doesn't!
      return (allSortFieldsAreStored) ? getMaxResults() + (int) getStartOffset() : Integer.MAX_VALUE;
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

      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      try (CloseableIterator<EntityEntry<Object, S>> entryIterator =
                 baseQuery.startOffset(0).maxResults(-1).local(local).entryIterator(false)) {
         Iterator<ObjectFilter.FilterResult> it =
               new MappingIterator<>(entryIterator, e -> objectFilter.filter(e.key(), e.value(), null));
         int count = 0;
         while (it.hasNext()) {
            ObjectFilter.FilterResult fr = it.next();
            Object removed = cache.remove(fr.getKey());
            if (removed != null) {
               count++;
            }
         }
         return count;
      } finally {
         if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);
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
