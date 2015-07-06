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
//todo [anistor] make local
public final class EmbeddedQuery extends BaseEmbeddedQuery {

   private final JPAFilterAndConverter filter;

   //todo [anistor] make local
   public EmbeddedQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, JPAFilterAndConverter filter,
                        long startOffset, int maxResults) {
      super(queryFactory, cache, filter.getJPAQuery(), filter.getObjectFilter().getProjection(), startOffset, maxResults);
      this.filter = filter;
   }

   @Override
   protected Comparator<Comparable[]> getComparator() {
      return filter.getObjectFilter().getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getIterator() {
      final CloseableIterator<Map.Entry<?, ObjectFilter.FilterResult>> it = cache.filterEntries(filter).converter(filter).iterator();
      return new CloseableIterator<ObjectFilter.FilterResult>() {

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
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }
}
