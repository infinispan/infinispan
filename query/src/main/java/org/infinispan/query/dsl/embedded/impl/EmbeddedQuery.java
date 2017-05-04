package org.infinispan.query.dsl.embedded.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.CacheFilters;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.QueryFactory;


/**
 * Non-indexed embedded-mode query.
 *
 * @author anistor@redhat,com
 * @since 7.0
 */
final class EmbeddedQuery extends BaseEmbeddedQuery {

   private final QueryEngine queryEngine;

   private IckleFilterAndConverter<?, ?> filter;

   EmbeddedQuery(QueryEngine queryEngine, QueryFactory queryFactory, AdvancedCache<?, ?> cache,
                 String queryString, Map<String, Object> namedParameters, String[] projection,
                 long startOffset, int maxResults) {
      super(queryFactory, cache, queryString, namedParameters, projection, startOffset, maxResults);
      this.queryEngine = queryEngine;
   }

   @Override
   public void resetQuery() {
      super.resetQuery();
      filter = null;
   }

   private IckleFilterAndConverter createFilter() {
      // filter is created first time only
      if (filter == null) {
         filter = queryEngine.createAndWireFilter(queryString, namedParameters);

         // force early validation!
         filter.getObjectFilter();
      }
      return filter;
   }

   @Override
   protected Comparator<Comparable[]> getComparator() {
      return createFilter().getObjectFilter().getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getIterator() {
      CacheStream<CacheEntry<?, ObjectFilter.FilterResult>> stream = (CacheStream<CacheEntry<?, ObjectFilter.FilterResult>>) CacheFilters.filterAndConvert(cache.cacheEntrySet().stream(), createFilter());
      return Closeables.iterator(stream.map(CacheEntry::getValue));
   }

   @Override
   public String toString() {
      return "EmbeddedQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }
}
