package org.infinispan;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link Stream} that has additional operations to monitor or control behavior when used from a {@link Cache}.  Note that
 * you may only use these additional methods on the CacheStream before any intermediate operations are performed as
 * a {@link Stream} is returned from those methods.
 *
 * <p>Whenever the iterator or spliterator methods are used the user <b>must</b> close the {@link Stream}
 * that the method was invoked on after completion of its operation.  Failure to do so may cause a thread leakage if
 * the iterator or spliterator are not fully consumed.</p>
 *
 * <p>When using stream that is backed by a distributed cache these operations will be performed using remote
 * distribution controlled by the segments that each key maps to.  All intermediate operations are lazy, even the
 * special cases described in later paragraphs and are not evaluated until a final terminal operation is invoked on
 * the stream.  Essentially each set of intermediate operations is shipped to each remote node where they are applied
 * to a local stream there and finally the terminal operation is completed.  If this stream is parallel the processing
 * on remote nodes is also done using a parallel stream.</p>
 *
 * <p>Parallel distribution is enabled by default for all operations except for {@link CacheStream#iterator()} &
 * {@link CacheStream#spliterator()}.  Please see {@link CacheStream#sequentialDistribution()} and
 * {@link CacheStream#parallelDistribution()}.  With this disabled only a single node will process the operation
 * at a time (includes locally).</p>
 *
 * <p>Rehash aware is enabled by default for all operations which will provide guaranteed consistency for all operations
 * except for {@link CacheStream#forEach(Consumer)}.  Please the method above for details about its consistency
 * guarantees.  If you wish to disable rehash aware operations you can disable them by calling
 * {@link CacheStream#disableRehashAware()} which should provide better performance for some operations.  The
 * performance is most affected for the key aware operations {@link CacheStream#iterator()},
 * {@link CacheStream#spliterator()}, {@link CacheStream#forEach(Consumer)}</p>
 *
 * <p>Some terminal operators are special in that they act like an intermediate iterator operation.  That is that
 * it is an intermediate operation, but it requires processing the results using an interator intermediately before
 * the stream can complete.</p>
 *
 * <p>A good example of an intermediate iterator operation is using distinct intermediate operation. What will happen
 * is upon calling the terminal operation an iterator operation will be ran using all of
 * the intermediate operations up to the distinct operation remotely.  This iterator is then used to fuel a local
 * stream where all of the remaining intermediate operations are performed and then finally the terminal operation is
 * applied as normal.  Note in this case the intermediate iterator still obeys the
 * {@link CacheStream#distributedBatchSize(int)} setting irrespective of the terminal operator.</p>
 *
 * @param <R> The type of the stream
 * @since 8.0
 */
public interface CacheStream<R> extends Stream<R> {
   /**
    * This would disable sending requests to all other remote nodes compared to one at a time. This can reduce memory
    * pressure on the originator node at the cost of performance.
    * <p>Parallel distribution is enabled by default except for {@link CacheStream#iterator()} &
    * {@link CacheStream#spliterator()}</p>
    * @return This stream again with parallel distribution disabled
    */
   CacheStream<R> sequentialDistribution();

   /**
    * This would enable sending requests to all other remote nodes when a terminal operator is performed.  This
    * requires additional overhead as it must process results concurrently from various nodes, but should perform
    * faster in the majority of cases.
    * <p>Parallel distribution is enabled by default except for {@link CacheStream#iterator()} &
    * {@link CacheStream#spliterator()}</p>
    * @return This stream again with parallel distribution enabled.
    */
   CacheStream<R> parallelDistribution();

   /**
    * Filters which entries are returned by what segment they are present in.  This method can be substantially more
    * efficient then using a regular {@link CacheStream#filter(Predicate)} method as this can control what nodes are
    * asked for data and what entries are read from the underlying CacheStore if present.
    * @param segments The segments to use for this stream operation.  Any segments not in this set will be ignored.
    * @return This stream again with the segments filtered.
    */
   CacheStream<R> filterKeySegments(Set<Integer> segments);

   /**
    * Filters which entries are returned by only returning ones that map to the given key.  This method will <b>always</b>
    * be faster than a regular {@link CacheStream#filter(Predicate)} if any keys must be retrieved remotely or if a
    * cache store is in use.
    * @param keys The keys that this stream will only operate on.
    * @return This stream again with the keys filtered.
    */
   CacheStream<R> filterKeys(Set<?> keys);

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
    * @return This stream again with the batch size updated
    */
   CacheStream<R> distributedBatchSize(int batchSize);

   /**
    * Allows registration of a segment completion listener that is notified when a segment has completed
    * processing.  If the terminal operator has a short circuit this listener may never be called.
    * <p>This method is designed for the sole purpose of use with the {@link CacheStream#iterator()} to allow for
    * a user to track completion of segments as they are returned from the iterator.  Behavior of other methods
    * is not specified.  Please see {@link CacheStream#iterator()} for more information.</p>
    * <p>Multiple listeners may be registered upon multiple invocations of this method.  The ordering of notified
    * listeners is not specified.</p>
    * @param listener The listener that will be called back as segments are completed.
    * @return This stream again with the listener registered.
    */
   CacheStream<R> segmentCompletionListener(SegmentCompletionListener listener);

   /**
    * Disables tracking of rehash events that could occur to the underlying cache.  If a rehash event occurs while
    * a terminal operation is being performed it is possible for some values that are in the cache to not be found.
    * Note that you will never have an entry duplicated when rehash awareness is disabled, only lost values.
    * <p>Most terminal operations will run faster with rehash awareness disabled even without a rehash occuring.
    * However if a rehash occurs with this disabled be prepared to possibly receive only a subset of values.</p>
    * @return This stream again with rehash awareness disabled.
    */
   CacheStream<R> disableRehashAware();

   /**
    * Functional interface that is used as a callback when segments are completed.  Please see
    * {@link CacheStream#segmentCompletionListener(SegmentCompletionListener)} for more details.
    * @since 8.0
    */
   @FunctionalInterface
   interface SegmentCompletionListener {
      /**
       * Method invoked when the segment has been found to be consumed properly by the terminal operation.
       * @param segments The segments that were completed
       */
      void segmentCompleted(Set<Integer> segments);
   }

   /**
    * {@inheritDoc}
    * <p>This operation is performed remotely on the node that is the primary owner for the key tied to the entry(s)
    * in this stream.</p>
    * <p>NOTE: This method while being rehash aware has the lowest consistency of all of the operators.  This
    * operation will be performed on every entry at least once in the cluster, as long as the originator doesn't go
    * down while it is being performed.  This is due to how the distributed action is performed.  Essentially the
    * {@link CacheStream#distributedBatchSize} value controls how many elements are processed per node at a time
    * when rehash is enabled. After those are complete the keys are sent to the originator to confirm that those were
    * processed.  If that node goes down during/before the response those keys will be processed a second time.</p>
    * <p>This method is ran distributed by default with a distributed backing cache.  However if you wish for this
    * operation to run locally you can use the {@link CacheStream#iterator()} method to return all of the results
    * locally and then use {@link Iterator#forEachRemaining(Consumer)} method for a single threaded variant.  If you
    * wish to have a parallel variant you can use {@link java.util.stream.StreamSupport#stream(Spliterator, boolean)}
    * passing in the spliterator from the stream.  In either case remember you <b>must</b> close the stream after
    * you are done processing the iterator or spliterator..</p>
    * @param action
    */
   @Override
   void forEach(Consumer<? super R> action);

   /**
    * {@inheritDoc}
    * <p>Usage of this operator requires closing this stream after you are done with the iterator.  The preferred
    * usage is to use a try with resource block on the stream.</p>
    * <p>This method has special usage with the {@link org.infinispan.CacheStream.SegmentCompletionListener} in
    * that as entries are retrieved from the next method it will complete segments.</p>
    * <p>This method obeys the {@link CacheStream#distributedBatchSize(int)} setting by only ever returning the
    * elements that mapped to that many keys.  Note that when using methods such as
    * {@link CacheStream#flatMap(Function)} that you will have possibly more than 1 element mapped to a given key
    * so this doesn't guarantee that many number of entries are returned per batch.</p>
    * @return the element iterator for this stream
    */
   @Override
   Iterator<R> iterator();

   /**
    * {@inheritDoc}
    * <p>Usage of this operator requires closing this stream after you are done with the spliterator.  The preferred
    * usage is to use a try with resource block on the stream.</p>
    * @return the element spliterator for this stream
    */
   @Override
   Spliterator<R> spliterator();

   /**
    * {@inheritDoc}
    * <p>This method has special usage when used with a distributed cache backing this set.  This operation will act
    * as an intermediate iterator operation requiring data be brought locally for proper behavior.  This is
    * described in more detail in the {@link CacheStream} documentation</p>
    * <p>This intermediate iterator operation will be performed locally only requiring all elements to be in memory</p>
    * @return the new stream
    */
   @Override
   Stream<R> sorted();

   /**
    * {@inheritDoc}
    * <p>This method has special usage when used with a distributed cache backing this set.  This operation will act
    * as an intermediate iterator operation requiring data be brought locally for proper behavior.  This is
    * described in more detail in the {@link CacheStream} documentation</p>
    * <p>This intermediate iterator operation will be performed locally only requiring all elements to be in memory</p>
    * @param comparator the comparator to be used for sorting the elements
    * @return the new stream
    */
   @Override
   Stream<R> sorted(Comparator<? super R> comparator);

   /**
    * {@inheritDoc}
    * <p>This method has special usage when used with a distributed cache backing this set.  This operation will act
    * as an intermediate iterator operation requiring data be brought locally for proper behavior.  This is
    * described in more detail in the {@link CacheStream} documentation</p>
    * <p>This intermediate iterator operation will be performed both remotely and locally to reduce how many elements
    * are sent back from each node.</p>
    * @param maxSize how many elements to limit this stream to.
    * @return the new stream
    */
   @Override
   Stream<R> limit(long maxSize);

   /**
    * {@inheritDoc}
    * <p>This method has special usage when used with a distributed cache backing this set.  This operation will act
    * as an intermediate iterator operation requiring data be brought locally for proper behavior.  This is
    * described in more detail in the {@link CacheStream} documentation</p>
    * <p>This intermediate iterator operation will only be performed locally, however it will only have elements in
    * memory controlled by the {@link CacheStream#distributedBatchSize(int)} unless the terminal operator holds them.</p>
    * @param n how many elements to skip from this stream
    * @return the new stream
    */
   @Override
   Stream<R> skip(long n);

   /**
    * {@inheritDoc}
    * <p>This method has special usage when used with a distributed cache backing this set.  This operation will act
    * as an intermediate iterator operation requiring data be brought locally for proper behavior.  This is
    * described in more detail in the {@link CacheStream} documentation</p>
    * <p>This intermediate iterator operation will be performed locally and remotely requiring possibly a subset of
    * all elements to be in memory</p>
    * @return the new stream
    */
   @Override
   Stream<R> distinct();
}
