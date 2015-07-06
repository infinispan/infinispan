package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An embedded non-indexed query on top of the results returned by another query (usually Lucene based).
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
//todo [anistor] make local
public final class HybridQuery extends BaseEmbeddedQuery {

   private final ObjectFilter objectFilter;

   private final Query baseQuery;

   //todo [anistor] make local
   public HybridQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String jpaQuery, ObjectFilter objectFilter, long startOffset, int maxResults, Query baseQuery) {
      super(queryFactory, cache, jpaQuery, objectFilter.getProjection(), startOffset, maxResults);
      this.objectFilter = objectFilter;
      this.baseQuery = baseQuery;
   }

   @Override
   protected Comparator<Comparable[]> getComparator() {
      return objectFilter.getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getIterator() {
      final Iterator<Object> it = baseQuery.list().iterator();

      return new CloseableIterator<ObjectFilter.FilterResult>() {

         private ObjectFilter.FilterResult nextResult = null;

         private boolean ready = false;

         @Override
         public void close() {
         }

         @Override
         public boolean hasNext() {
            update();
            return nextResult != null;
         }

         @Override
         public ObjectFilter.FilterResult next() {
            if (hasNext()) {
               ready = false;
               return nextResult;
            } else {
               throw new NoSuchElementException();
            }
         }

         private void update() {
            if (!ready) {
               if (it.hasNext()) {
                  Object next = it.next();
                  nextResult = objectFilter.filter(next);
               } else {
                  nextResult = null;
               }
               ready = true;
            }
         }
      };
   }

   @Override
   public String toString() {
      return "HybridQuery{" +
            "jpaQuery=" + jpaQuery +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", baseQuery=" + baseQuery +
            '}';
   }
}
