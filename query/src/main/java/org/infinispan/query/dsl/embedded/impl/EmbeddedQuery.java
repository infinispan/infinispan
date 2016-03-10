package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.CacheFilters;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.QueryFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;


/**
 * Non-indexed embedded-mode query.
 *
 * @author anistor@redhat,com
 * @since 7.0
 */
final class EmbeddedQuery extends BaseEmbeddedQuery {

   private final QueryEngine queryEngine;

   private JPAFilterAndConverter<?, ?> filter;

   EmbeddedQuery(QueryEngine queryEngine, QueryFactory queryFactory, AdvancedCache<?, ?> cache,
                 String jpaQuery, Map<String, Object> namedParameters, String[] projection,
                 long startOffset, int maxResults) {
      super(queryFactory, cache, jpaQuery, namedParameters, projection, startOffset, maxResults);
      this.queryEngine = queryEngine;
   }

   @Override
   public void resetQuery() {
      super.resetQuery();
      filter = null;
   }

   private JPAFilterAndConverter createFilter() {
      // filter is created first time only
      if (filter == null) {
         filter = queryEngine.makeFilter(jpaQuery, namedParameters);

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
      Stream<CacheEntry<?, ObjectFilter.FilterResult>> stream = CacheFilters.filterAndConvert(cache.cacheEntrySet().stream(), createFilter());
      return Closeables.iterator(stream.map(e -> e.getValue()));
   }

   @Override
   public String toString() {
      return "EmbeddedQuery{" +
            "jpaQuery=" + jpaQuery +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }
}
