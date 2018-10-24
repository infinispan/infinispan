package org.infinispan.query.dsl.embedded.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.util.function.SerializablePredicate;


/**
 * Non-indexed embedded-mode query.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class EmbeddedQuery extends BaseEmbeddedQuery {

   private static final SerializablePredicate<ObjectFilter.FilterResult> NON_NULL_PREDICATE = Objects::nonNull;

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
   protected Comparator<Comparable[]> getComparator() {
      return createFilter().getObjectFilter().getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getIterator() {
      IckleFilterAndConverter<Object, Object> ickleFilter = (IckleFilterAndConverter<Object, Object>) createFilter();
      CacheStream<Map.Entry<Object, Object>> entryStream = ((AdvancedCache<Object, Object>) cache).entrySet().stream();
      CacheStream<ObjectFilter.FilterResult> resultStream = entryStream.map(ickleFilter).filter(NON_NULL_PREDICATE);
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
            '}';
   }
}
