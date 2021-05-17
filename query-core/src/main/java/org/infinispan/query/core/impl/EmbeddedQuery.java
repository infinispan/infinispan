package org.infinispan.query.core.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.filter.CacheFilters;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryFactory;


/**
 * Non-indexed embedded-mode query.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class EmbeddedQuery<T> extends BaseEmbeddedQuery<T> {

   private final QueryEngine<?> queryEngine;

   private IckleFilterAndConverter<?, ?> filter;

   public EmbeddedQuery(QueryEngine<?> queryEngine, QueryFactory queryFactory, AdvancedCache<?, ?> cache,
                        String queryString, Map<String, Object> namedParameters, String[] projection,
                        long startOffset, int maxResults, LocalQueryStatistics queryStatistics, boolean local) {
      super(queryFactory, cache, queryString, namedParameters, projection, startOffset, maxResults, queryStatistics, local);
      this.queryEngine = queryEngine;
   }

   @Override
   public void resetQuery() {
      super.resetQuery();
      filter = null;
   }

   @Override
   protected void recordQuery(Long time) {
      queryStatistics.nonIndexedQueryExecuted(queryString, time);
   }

   private IckleFilterAndConverter<?, ?> createFilter() {
      // filter is created first time only, or again if reset was called meanwhile
      if (filter == null) {
         filter = queryEngine.createAndWireFilter(queryString, namedParameters);

         // force early query validation, at creation time rather than deferring to execution time
         filter.getObjectFilter();
      }
      return filter;
   }

   @Override
   protected Comparator<Comparable<?>[]> getComparator() {
      return createFilter().getObjectFilter().getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getInternalIterator() {
      IckleFilterAndConverter<Object, Object> ickleFilter = (IckleFilterAndConverter<Object, Object>) createFilter();
      AdvancedCache<Object, Object> cache = (AdvancedCache<Object, Object>) this.cache;

      if (isLocal()) cache = cache.withFlags(Flag.CACHE_MODE_LOCAL);

      CacheStream<CacheEntry<Object, Object>> entryStream = (cache).cacheEntrySet().stream();
      if (timeout > 0) {
         entryStream = entryStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }
      CacheStream<ObjectFilter.FilterResult> resultStream = CacheFilters.filterAndConvertToValue(entryStream, ickleFilter);
      if (timeout > 0) {
         resultStream = resultStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }
      return Closeables.iterator(resultStream);
   }

   @Override
   public String toString() {
      return "EmbeddedQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            '}';
   }
}
