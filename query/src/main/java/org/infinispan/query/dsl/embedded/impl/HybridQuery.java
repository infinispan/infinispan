package org.infinispan.query.dsl.embedded.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

/**
 * A non-indexed query performed on top of the results returned by another query (usually a Lucene based query). This
 * mechanism is used to implement hybrid two-stage queries that perform an index query using a partial query using only
 * the indexed fields and then filter the result again in memory with the full filter.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
class HybridQuery extends BaseEmbeddedQuery {

   protected final ObjectFilter objectFilter;

   protected final Query baseQuery;

   HybridQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString, Map<String, Object> namedParameters,
               ObjectFilter objectFilter,
               long startOffset, int maxResults,
               Query baseQuery) {
      super(queryFactory, cache, queryString, namedParameters, objectFilter.getProjection(), startOffset, maxResults);
      this.objectFilter = objectFilter;
      this.baseQuery = baseQuery;
   }

   @Override
   protected Comparator<Comparable[]> getComparator() {
      return objectFilter.getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getIterator() {
      return new CloseableIterator<ObjectFilter.FilterResult>() {

         private final Iterator<?> it = getBaseIterator();

         private ObjectFilter.FilterResult nextResult = null;

         private boolean isReady = false;

         @Override
         public void close() {
         }

         @Override
         public boolean hasNext() {
            updateNext();
            return nextResult != null;
         }

         @Override
         public ObjectFilter.FilterResult next() {
            updateNext();
            if (nextResult != null) {
               ObjectFilter.FilterResult next = nextResult;
               isReady = false;
               nextResult = null;
               return next;
            } else {
               throw new NoSuchElementException();
            }
         }

         private void updateNext() {
            if (!isReady) {
               while (it.hasNext()) {
                  Object next = it.next();
                  nextResult = objectFilter.filter(next);
                  if (nextResult != null) {
                     break;
                  }
               }
               isReady = true;
            }
         }
      };
   }

   protected Iterator<?> getBaseIterator() {
      return baseQuery.list().iterator();
   }

   @Override
   public String toString() {
      return "HybridQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", baseQuery=" + baseQuery +
            '}';
   }
}
