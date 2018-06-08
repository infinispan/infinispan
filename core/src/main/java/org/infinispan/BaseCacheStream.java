package org.infinispan;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.BaseStream;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;

/**
 * Interface that defines the base methods of all streams returned from a {@link Cache}.  This interface
 * is useful to hold a reference to any of the types while still being able to invoke some methods.
 * @author wburns
 * @since 9.0
 */
public interface BaseCacheStream<T, S extends BaseStream<T, S>> extends BaseStream<T, S> {
   /**
    * This would disable sending requests to all other remote nodes compared to one at a time. This can reduce memory
    * pressure on the originator node at the cost of performance.
    * <p>Parallel distribution is enabled by default except for {@link CacheStream#iterator()} &
    * {@link CacheStream#spliterator()}</p>
    * @return a stream with parallel distribution disabled
    */
   BaseCacheStream sequentialDistribution();

   /**
    * This would enable sending requests to all other remote nodes when a terminal operator is performed.  This
    * requires additional overhead as it must process results concurrently from various nodes, but should perform
    * faster in the majority of cases.
    * <p>Parallel distribution is enabled by default except for {@link CacheStream#iterator()} &
    * {@link CacheStream#spliterator()}</p>
    * @return a stream with parallel distribution enabled.
    */
   BaseCacheStream parallelDistribution();

   /**
    * Filters which entries are returned by what segment they are present in.  This method can be substantially more
    * efficient than using a regular {@link CacheStream#filter(Predicate)} method as this can control what nodes are
    * asked for data and what entries are read from the underlying CacheStore if present.
    * @param segments The segments to use for this stream operation.  Any segments not in this set will be ignored.
    * @return a stream with the segments filtered.
    * @deprecated since 9.3 This is to be replaced by {@link #filterKeySegments(IntSet)}
    */
   BaseCacheStream filterKeySegments(Set<Integer> segments);

   /**
    * Filters which entries are returned by what segment they are present in.  This method can be substantially more
    * efficient than using a regular {@link CacheStream#filter(Predicate)} method as this can control what nodes are
    * asked for data and what entries are read from the underlying CacheStore if present.
    * @param segments The segments to use for this stream operation.  Any segments not in this set will be ignored.
    * @return a stream with the segments filtered.
    * @since 9.3
    */
   BaseCacheStream filterKeySegments(IntSet segments);

   /**
    * Filters which entries are returned by only returning ones that map to the given key.  This method will
    * be faster than a regular {@link CacheStream#filter(Predicate)} if the filter is holding references to the same
    * keys.
    * @param keys The keys that this stream will only operate on.
    * @return a stream with the keys filtered.
    */
   BaseCacheStream filterKeys(Set<?> keys);

   /**
    * Controls how many keys are returned from a remote node when using a stream terminal operation with a distributed
    * cache to back this stream.  This value is ignored when terminal operators that don't track keys are used.  Key
    * tracking terminal operators are {@link CacheStream#iterator()}, {@link CacheStream#spliterator()},
    * {@link CacheStream#forEach(Consumer)}.  Please see those methods for additional information on how this value
    * may affect them.
    * <p>This value may be used in the case of a a terminal operator that doesn't track keys if an intermediate
    * operation is performed that requires bringing keys locally to do computations.  Examples of such intermediate
    * operations are {@link CacheStream#sorted()}, {@link CacheStream#sorted(Comparator)},
    * {@link CacheStream#distinct()}, {@link CacheStream#limit(long)}, {@link CacheStream#skip(long)}</p>
    * <p>This value is <b>always</b> ignored when this stream is backed by a cache that is not distributed as all
    * values are already local.</p>
    * @param batchSize The size of each batch.  This defaults to the state transfer chunk size.
    * @return a stream with the batch size updated
    */
   BaseCacheStream distributedBatchSize(int batchSize);

   /**
    * Allows registration of a segment completion listener that is notified when a segment has completed
    * processing.  If the terminal operator has a short circuit this listener may never be called.
    * <p>This method is designed for the sole purpose of use with the {@link CacheStream#iterator()} to allow for
    * a user to track completion of segments as they are returned from the iterator.  Behavior of other methods
    * is not specified.  Please see {@link CacheStream#iterator()} for more information.</p>
    * <p>Multiple listeners may be registered upon multiple invocations of this method.  The ordering of notified
    * listeners is not specified.</p>
    * <p>This is only used if this stream did not invoke {@link BaseCacheStream#disableRehashAware()} and has no
    * flat map based operations. If this is done no segments will be notified.</p>
    * @param listener The listener that will be called back as segments are completed.
    * @return a stream with the listener registered.
    */
   BaseCacheStream segmentCompletionListener(SegmentCompletionListener listener);

   /**
    * Disables tracking of rehash events that could occur to the underlying cache.  If a rehash event occurs while
    * a terminal operation is being performed it is possible for some values that are in the cache to not be found.
    * Note that you will never have an entry duplicated when rehash awareness is disabled, only lost values.
    * <p>Most terminal operations will run faster with rehash awareness disabled even without a rehash occuring.
    * However if a rehash occurs with this disabled be prepared to possibly receive only a subset of values.</p>
    * @return a stream with rehash awareness disabled.
    */
   BaseCacheStream disableRehashAware();

   /**
    * Sets a given time to wait for a remote operation to respond by.  This timeout does nothing if the terminal
    * operation does not go remote.
    * <p>If a timeout does occur then a {@link java.util.concurrent.TimeoutException} is thrown from the terminal
    * operation invoking thread or on the next call to the {@link Iterator} or {@link Spliterator}.</p>
    * <p>Note that if a rehash occurs this timeout value is reset for the subsequent retry if rehash aware is
    * enabled.</p>
    * @param timeout the maximum time to wait
    * @param unit the time unit of the timeout argument
    * @return a stream with the timeout set
    */
   BaseCacheStream timeout(long timeout, TimeUnit unit);

   /**
    * Functional interface that is used as a callback when segments are completed.  Please see
    * {@link BaseCacheStream#segmentCompletionListener(SegmentCompletionListener)} for more details.
    * @author wburns
    * @since 9.0
    */
   @FunctionalInterface
   interface SegmentCompletionListener extends Consumer<Supplier<PrimitiveIterator.OfInt>> {
      /**
       * Method invoked when the segment has been found to be consumed properly by the terminal operation.
       * @param segments The segments that were completed
       * @deprecated This method requires boxing for each segment. Please use {@link SegmentCompletionListener#accept(Supplier)} instead
       */
      @Deprecated
      void segmentCompleted(Set<Integer> segments);

      /**
       * Invoked each time a given number of segments have completed and the terminal operation has consumed all
       * entries in the given segment
       * @param segments The segments that were completed
       */
      default void accept(Supplier<PrimitiveIterator.OfInt> segments) {
         segmentCompleted(IntSets.from(segments.get()));
      }
   }
}
