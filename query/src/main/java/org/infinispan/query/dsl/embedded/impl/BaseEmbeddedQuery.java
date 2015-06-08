package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Base class for embedded-mode query implementations.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
//todo [anistor] make local
public abstract class BaseEmbeddedQuery extends BaseQuery {

   private static final int INITIAL_CAPACITY = 1000;

   protected final AdvancedCache<?, ?> cache;

   /**
    * The cached results, lazily evaluated.
    */
   private List results;

   private int resultSize;

   protected final int startOffset;

   protected final int maxResults;

   protected BaseEmbeddedQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String jpaQuery,
                               String[] projection, long startOffset, int maxResults) {
      super(queryFactory, jpaQuery, projection);

      ensureAccessPermissions(cache);

      this.cache = cache;
      this.startOffset = startOffset < 0 ? 0 : (int) startOffset;
      this.maxResults = maxResults;
   }

   private void ensureAccessPermissions(AdvancedCache<?, ?> cache) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
   }

   @Override
   public <T> List<T> list() {
      if (results == null) {
         results = listInternal();
      }
      return results;
   }

   private List listInternal() {
      List results;

      CloseableIterator<ObjectFilter.FilterResult> iterator = getIterator();
      Comparator<Comparable[]> comparator = getComparator();

      if (comparator == null) {
         // collect unsorted results and get the requested page if any was specified
         try {
            if (iterator.hasNext()) {
               results = new ArrayList(INITIAL_CAPACITY);
               while (iterator.hasNext()) {
                  ObjectFilter.FilterResult entry = iterator.next();
                  resultSize++;
                  if (resultSize > startOffset && (maxResults == -1 || results.size() < maxResults)) {
                     results.add(projection != null ? entry.getProjection() : entry.getInstance());
                  }
               }
            } else {
               results = Collections.emptyList();
            }
         } finally {
            try {
               iterator.close();
            } catch (Exception e) {
               // exception ignored
            }
         }
      } else {
         // collect and sort results, in reverse order for now
         PriorityQueue<ObjectFilter.FilterResult> filterResults = new PriorityQueue<ObjectFilter.FilterResult>(INITIAL_CAPACITY, new ReverseFilterResultComparator(comparator));
         try {
            while (iterator.hasNext()) {
               ObjectFilter.FilterResult entry = iterator.next();
               resultSize++;
               filterResults.add(entry);
               if (maxResults != -1 && filterResults.size() > startOffset + maxResults) {
                  // remove the head, which is actually the highest result
                  filterResults.remove();
               }
            }
         } finally {
            try {
               iterator.close();
            } catch (Exception e) {
               // exception ignored
            }
         }

         // collect and reverse
         if (filterResults.size() > startOffset) {
            Object[] res = new Object[filterResults.size() - startOffset];
            int i = filterResults.size();
            while (i-- > startOffset) {
               ObjectFilter.FilterResult r = filterResults.remove();
               res[i - startOffset] = projection != null ? r.getProjection() : r.getInstance();
            }
            results = Arrays.asList(res);
         } else {
            results = Collections.emptyList();
         }
      }

      return results;
   }

   protected abstract Comparator<Comparable[]> getComparator();

   protected abstract CloseableIterator<ObjectFilter.FilterResult> getIterator();

   @Override
   public int getResultSize() {
      list();
      return resultSize;
   }

   @Override
   public String toString() {
      return "BaseEmbeddedQuery{" +
            "jpaQuery=" + jpaQuery +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }

   private static class ReverseFilterResultComparator implements Comparator<ObjectFilter.FilterResult> {

      private final Comparator<Comparable[]> comparator;

      private ReverseFilterResultComparator(Comparator<Comparable[]> comparator) {
         this.comparator = comparator;
      }

      @Override
      public int compare(ObjectFilter.FilterResult o1, ObjectFilter.FilterResult o2) {
         return -comparator.compare(o1.getSortProjection(), o2.getSortProjection());
      }
   }
}
