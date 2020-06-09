package org.infinispan.query.core.impl;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.objectfilter.ObjectFilter.FilterResult;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * Base class for embedded-mode query implementations. Subclasses need to implement {@link #getInternalIterator()} and
 * {@link #getComparator()} methods and this class will take care of sorting (fully in-memory).
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public abstract class BaseEmbeddedQuery<T> extends BaseQuery<T> {

   private static final Log log = Logger.getMessageLogger(Log.class, BaseEmbeddedQuery.class.getName());

   /**
    * Initial capacity of the collection used for collecting results when performing internal sorting.
    */
   private static final int INITIAL_CAPACITY = 1000;

   protected final AdvancedCache<?, ?> cache;

   protected final PartitionHandlingSupport partitionHandlingSupport;

   protected BaseEmbeddedQuery(QueryFactory queryFactory, AdvancedCache<?, ?> cache, String queryString, Map<String, Object> namedParameters,
                               String[] projection, long startOffset, int maxResults) {
      super(queryFactory, queryString, namedParameters, projection, startOffset, maxResults);
      this.cache = cache;
      this.partitionHandlingSupport = new PartitionHandlingSupport(cache);
   }

   @Override
   public void resetQuery() {
   }

   @Override
   public List<T> list() {
      return execute().list();
   }

   @Override
   public QueryResult<T> execute() {
      partitionHandlingSupport.checkCacheAvailable();
      List<T> results = listInternal(getComparator());
      return new QueryResultImpl<>(results);
   }

   @Override
   public CloseableIterator<T> iterator() {
      partitionHandlingSupport.checkCacheAvailable();
      Comparator<Comparable<?>[]> comparator = getComparator();
      if (comparator == null) {
         MappingIterator<FilterResult, Object> iterator = new MappingIterator<>(getInternalIterator(), this::mapFilterResult)
               .limit(maxResults).skip(startOffset);
         return (CloseableIterator<T>) iterator;
      }
      List<T> results = listInternal(comparator);
      return Closeables.iterator(results.iterator());
   }

   private List<T> listInternal(Comparator<Comparable<?>[]> comparator) {
      List<Object> results;
      try (CloseableIterator<FilterResult> iterator = getInternalIterator()) {
         if (!iterator.hasNext()) {
            results = Collections.emptyList();
         } else {
            if (comparator == null) {
               results = StreamSupport.stream(spliteratorUnknownSize(new MappingIterator<>(iterator, s -> s).limit(maxResults).skip(startOffset), 0), false)
                                      .map(this::mapFilterResult)
                                      .collect(new TimedCollector<>(Collectors.toList(), timeout));
            } else {
               log.warnPerfSortedNonIndexed(queryString);
               // Collect and sort results in a PriorityQueue, in reverse order for now. We'll reverse them again before returning.
               // We keep the FilterResult wrapper in the queue rather than the actual value because we need FilterResult.getSortProjection() to perform sorting.
               PriorityQueue<FilterResult> queue = StreamSupport.stream(spliteratorUnknownSize(iterator, 0), false)
                                                                .collect(new TimedCollector<>(Collector.of(() -> new PriorityQueue<>(INITIAL_CAPACITY, new ReverseFilterResultComparator(comparator)),
                                                                                                           this::addToPriorityQueue, (q1, q2) -> q1, IDENTITY_FINISH), timeout));

               // trim the results that are outside of the requested range and reverse them
               if (queue.size() > startOffset) {
                  Object[] res = new Object[queue.size() - startOffset];
                  int i = queue.size();
                  while (i-- > startOffset) {
                     FilterResult r = queue.remove();
                     res[i - startOffset] = mapFilterResult(r);
                  }
                  results = Arrays.asList(res);
               } else {
                  results = Collections.emptyList();
               }
            }
         }
      }

      return (List<T>) results;
   }

   private void addToPriorityQueue(PriorityQueue<FilterResult> queue, FilterResult filterResult) {
      queue.add(filterResult);
      if (maxResults != -1 && queue.size() > startOffset + maxResults) {
         // remove the head, which is actually the lowest ranking result because the queue is reversed (initially)
         queue.remove();
      }
   }

   /**
    * Create a comparator to be used for ordering the results returned by {@link #getInternalIterator()}.
    *
    * @return the comparator or {@code null} if no sorting needs to be applied
    */
   protected abstract Comparator<Comparable<?>[]> getComparator();

   /**
    * Create an iterator over the results of the query, in no particular order. Ordering will be provided if {@link
    * #getComparator()} returns a non-null {@link Comparator}. Please note this it not the same iterator as the one
    * retuend by {@link #iterator()}.
    */
   protected abstract CloseableIterator<FilterResult> getInternalIterator();

   @Override
   public int getResultSize() {
      int count = 0;
      try (CloseableIterator<?> iterator = getInternalIterator()) {
         while (iterator.hasNext()) {
            iterator.next();
            count++;
         }
      }
      return count;
   }

   private Object mapFilterResult(FilterResult result) {
      return projection != null ? result.getProjection() : result.getInstance();
   }

   @Override
   public String toString() {
      return "BaseEmbeddedQuery{" +
            "queryString=" + queryString +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", timeout=" + timeout +
            '}';
   }

   /**
    * Compares two {@link FilterResult} objects based on a given {@link Comparator} and reverses the
    * result.
    */
   private static final class ReverseFilterResultComparator implements Comparator<FilterResult> {

      private final Comparator<Comparable<?>[]> comparator;

      private ReverseFilterResultComparator(Comparator<Comparable<?>[]> comparator) {
         this.comparator = comparator;
      }

      @Override
      public int compare(FilterResult o1, FilterResult o2) {
         return -comparator.compare(o1.getSortProjection(), o2.getSortProjection());
      }
   }
}
