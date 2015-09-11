package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.QueryFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;


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

   private JPAFilterAndConverter<?, ?> createFilter() {
      // filter is created first time only
      if (filter == null) {
         filter = queryEngine.makeFilter(jpaQuery, namedParameters);

         // force early validation!
         filter.getObjectFilter();
      }
      return filter;
   }

   private CloseableIterator<Map.Entry<?, ObjectFilter.FilterResult>> createFilteredIterator() {
      JPAFilterAndConverter f = createFilter();
      return cache.filterEntries(f).converter(f).iterator();
   }

   @Override
   protected Comparator<Comparable[]> getComparator() {
      return createFilter().getObjectFilter().getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getIterator() {
      return new CloseableIterator<ObjectFilter.FilterResult>() {

         private final CloseableIterator<Map.Entry<?, ObjectFilter.FilterResult>> it = createFilteredIterator();

         @Override
         public void remove() {
            throw new UnsupportedOperationException("remove");
         }

         @Override
         public boolean hasNext() {
            return it.hasNext();
         }

         @Override
         public ObjectFilter.FilterResult next() {
            return it.next().getValue();
         }

         @Override
         public void close() {
            it.close();
         }
      };
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
