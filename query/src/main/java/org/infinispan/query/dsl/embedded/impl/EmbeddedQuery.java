package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.dsl.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;


/**
 * Non-indexed embedded-mode query.
 *
 * @author anistor@redhat,com
 * @since 7.0
 */
public final class EmbeddedQuery implements Query {

   private static final int INITIAL_CAPACITY = 1000;

   private final AdvancedCache<?, ?> cache;

   private final FilterAndConverter filter;

   private List results;

   private int resultSize;

   private final String[] projection;

   private final int startOffset;

   private final int maxResults;

   public EmbeddedQuery(AdvancedCache<?, ?> cache, String jpaQuery, long startOffset, int maxResults, Class<? extends Matcher> matcherImplClass) {
      this.cache = cache;
      this.startOffset = startOffset < 0 ? 0 : (int) startOffset;
      this.maxResults = maxResults;

      filter = new FilterAndConverter(jpaQuery, matcherImplClass);
      filter.injectDependencies(cache);

      // this also triggers early validation
      projection = filter.getObjectFilter().getProjection();
   }

   public String[] getProjection() {
      return projection;
   }

   @Override
   public <T> List<T> list() {
      if (results == null) {
         CloseableIterable<Map.Entry<?, ObjectFilter.FilterResult>> iterable = cache.filterEntries(filter).converter(filter);
         Comparator<Comparable[]> comparator = filter.getObjectFilter().getComparator();
         if (comparator == null) {
            // collect unsorted results and get the requested page if any was specified
            results = new ArrayList<T>(INITIAL_CAPACITY);
            try {
               for (Map.Entry<?, ObjectFilter.FilterResult> entry : iterable) {
                  resultSize++;
                  if (resultSize > startOffset && (maxResults == -1 || results.size() < maxResults)) {
                     ObjectFilter.FilterResult r = entry.getValue();
                     results.add(projection != null ? r.getProjection() : r.getInstance());
                  }
               }
            } finally {
               try {
                  iterable.close();
               } catch (Exception e) {
                  // exception ignored
               }
            }
         } else {
            // collect and sort results, in reverse order for now
            PriorityQueue<ObjectFilter.FilterResult> filterResults = new PriorityQueue<ObjectFilter.FilterResult>(INITIAL_CAPACITY, new ReverseFilterResultComparator(comparator));
            try {
               for (Map.Entry<?, ObjectFilter.FilterResult> entry : iterable) {
                  resultSize++;
                  filterResults.add(entry.getValue());
                  if (maxResults != -1 && filterResults.size() > startOffset + maxResults) {
                     // remove the head, which is actually the highest result
                     filterResults.remove();
                  }
               }
            } finally {
               try {
                  iterable.close();
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
      }

      return results;
   }

   @Override
   public int getResultSize() {
      list();
      return resultSize;
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
