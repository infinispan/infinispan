package org.infinispan.query.dsl.embedded.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.impl.PartitionHandlingSupport;

/**
 * Base class for embedded-mode query implementations. Subclasses need to implement {@link #getIterator()} and {@link
 * #getComparator()} methods and this class will take care of sorting (fully in-memory).
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
abstract class BaseEmbeddedQuery extends BaseQuery {

   /**
    * Initial capacity of the collection used for collecting results when performing internal sorting.
    */
   private static final int INITIAL_CAPACITY = 1000;

   protected final AdvancedCache<?, ?> cache;
   protected final PartitionHandlingSupport partitionHandlingSupport;

   /**
    * The cached results, lazily evaluated.
    */
   private List<Object> results;

   /**
    * The total number of results matching the query, ignoring pagination. This is lazily evaluated.
    */
   private int resultSize;

   protected BaseEmbeddedQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString, Map<String, Object> namedParameters,
                               String[] projection, long startOffset, int maxResults) {
      super(queryFactory, queryString, namedParameters, projection, startOffset, maxResults);
      this.cache = cache;
      this.partitionHandlingSupport = new PartitionHandlingSupport(cache);
   }

   @Override
   public void resetQuery() {
      results = null;
   }

   @Override
   public <T> List<T> list() {
      partitionHandlingSupport.checkCacheAvailable();
      if (results == null) {
         results = listInternal();
      }
      return (List<T>) results;
   }

   private List<Object> listInternal() {
      List<Object> results;

      try (CloseableIterator<ObjectFilter.FilterResult> iterator = getIterator()) {
         if (!iterator.hasNext()) {
            results = Collections.emptyList();
         } else {
            Comparator<Comparable[]> comparator = getComparator();
            if (comparator == null) {
               // collect unsorted results and get the requested page if any was specified
               results = new ArrayList<>(INITIAL_CAPACITY);
               while (iterator.hasNext()) {
                  ObjectFilter.FilterResult entry = iterator.next();
                  resultSize++;
                  if (resultSize > startOffset && (maxResults == -1 || results.size() < maxResults)) {
                     results.add(projection != null ? entry.getProjection() : entry.getInstance());
                  }
               }
            } else {
               // collect and sort results, in reverse order for now
               PriorityQueue<ObjectFilter.FilterResult> filterResults = new PriorityQueue<>(INITIAL_CAPACITY, new ReverseFilterResultComparator(comparator));
               while (iterator.hasNext()) {
                  ObjectFilter.FilterResult entry = iterator.next();
                  resultSize++;
                  filterResults.add(entry);
                  if (maxResults != -1 && filterResults.size() > startOffset + maxResults) {
                     // remove the head, which is actually the highest result
                     filterResults.remove();
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
         }
      }

      return results;
   }

   /**
    * Create a comparator to be used for ordering the results returned by {@link #getIterator()}.
    *
    * @return the comparator or {@code null} if no sorting needs to be applied
    */
   protected abstract Comparator<Comparable[]> getComparator();

   /**
    * Create an iterator over the results of the query, in no particular order. Ordering will be provided if {@link
    * #getComparator()} returns a non-null {@link Comparator}.
    */
   protected abstract CloseableIterator<ObjectFilter.FilterResult> getIterator();

   @Override
   public int getResultSize() {
      list();
      return resultSize;
   }

   @Override
   public String toString() {
      return "BaseEmbeddedQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            '}';
   }

   /**
    * Compares two {@link ObjectFilter.FilterResult} objects based on a given {@link Comparator} and reverses the
    * result.
    */
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
