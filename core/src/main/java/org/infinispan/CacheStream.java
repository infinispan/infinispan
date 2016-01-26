package org.infinispan;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
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
 * <p>Rehash aware is enabled by default for all operations.  Any intermediate or terminal operation may be invoked
 * multiple times during a rehash and thus you should ensure the are idempotent.  This can be problematic for
 * {@link CacheStream#forEach(Consumer)} as it may be difficult to implement with such requirements, please see it for
 * more information.  If you wish to disable rehash aware operations you can disable them by calling
 * {@link CacheStream#disableRehashAware()} which should provide better performance for some operations.  The
 * performance is most affected for the key aware operations {@link CacheStream#iterator()},
 * {@link CacheStream#spliterator()}, {@link CacheStream#forEach(Consumer)}.  Disabling rehash can cause
 * incorrect results if the terminal operation is invoked and a rehash occurs before the operation completes.  If
 * incorrect results do occur it is guaranteed that it will only be that entries were missed and no entries are
 * duplicated.</p>
 *
 * <p>Any stateful intermediate operation requires pulling all information up to that point local to operate properly.
 * Each of these methods may have slightly different behavior, so make sure you check the method you are utilizing.</p>
 *
 * <p>An example of such an operation is using distinct intermediate operation. What will happen
 * is upon calling the terminal operation a remote retrieval operation will be ran using all of
 * the intermediate operations up to the distinct operation remotely.  This retrieval is then used to fuel a local
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
    * @return a stream with parallel distribution disabled
    */
   CacheStream<R> sequentialDistribution();

   /**
    * This would enable sending requests to all other remote nodes when a terminal operator is performed.  This
    * requires additional overhead as it must process results concurrently from various nodes, but should perform
    * faster in the majority of cases.
    * <p>Parallel distribution is enabled by default except for {@link CacheStream#iterator()} &
    * {@link CacheStream#spliterator()}</p>
    * @return a stream with parallel distribution enabled.
    */
   CacheStream<R> parallelDistribution();

   /**
    * Filters which entries are returned by what segment they are present in.  This method can be substantially more
    * efficient than using a regular {@link CacheStream#filter(Predicate)} method as this can control what nodes are
    * asked for data and what entries are read from the underlying CacheStore if present.
    * @param segments The segments to use for this stream operation.  Any segments not in this set will be ignored.
    * @return a stream with the segments filtered.
    */
   CacheStream<R> filterKeySegments(Set<Integer> segments);

   /**
    * Filters which entries are returned by only returning ones that map to the given key.  This method will <b>always</b>
    * be faster than a regular {@link CacheStream#filter(Predicate)} if any keys must be retrieved remotely or if a
    * cache store is in use.
    * @param keys The keys that this stream will only operate on.
    * @return a stream with the keys filtered.
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
    * @return a stream with the batch size updated
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
    * @return a stream with the listener registered.
    */
   CacheStream<R> segmentCompletionListener(SegmentCompletionListener listener);

   /**
    * Disables tracking of rehash events that could occur to the underlying cache.  If a rehash event occurs while
    * a terminal operation is being performed it is possible for some values that are in the cache to not be found.
    * Note that you will never have an entry duplicated when rehash awareness is disabled, only lost values.
    * <p>Most terminal operations will run faster with rehash awareness disabled even without a rehash occuring.
    * However if a rehash occurs with this disabled be prepared to possibly receive only a subset of values.</p>
    * @return a stream with rehash awareness disabled.
    */
   CacheStream<R> disableRehashAware();

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
   CacheStream<R> timeout(long timeout, TimeUnit unit);

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
    * <p>It is possible to have the cache local to each node injected into this instance if the provided
    * Consumer also implements the {@link org.infinispan.stream.CacheAware} interface.  This method will be invoked
    * before the consumer <code>accept()</code> method is invoked.</p>
    * <p>This method is ran distributed by default with a distributed backing cache.  However if you wish for this
    * operation to run locally you can use the {@code stream().iterator().forEachRemaining(action)} for a single
    * threaded variant.  If you
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
    * <p>This method obeys the {@link CacheStream#distributedBatchSize(int)}.  Note that when using methods such as
    * {@link CacheStream#flatMap(Function)} that you will have possibly more than 1 element mapped to a given key
    * so this doesn't guarantee that many number of entries are returned per batch.</p>
    * <p>Note that the {@link Iterator#remove()} method is only supported if no intermediate operations have been
    * applied to the stream and this is not a stream created from a {@link Cache#values()} collection.</p>
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
    * <p>This operation is performed entirely on the local node irrespective of the backing cache.  This
    * operation will act as an intermediate iterator operation requiring data be brought locally for proper behavior.
    * Beware this means it will require having all entries of this cache into memory at one time.  This is described in
    * more detail at {@link CacheStream}</p>
    * <p>Any subsequent intermediate operations and the terminal operation are also performed locally.</p>
    * @return the new stream
    */
   @Override
   Stream<R> sorted();

   /**
    * {@inheritDoc}
    * <p>This operation is performed entirely on the local node irrespective of the backing cache.  This
    * operation will act as an intermediate iterator operation requiring data be brought locally for proper behavior.
    * Beware this means it will require having all entries of this cache into memory at one time.  This is described in
    * more detail at {@link CacheStream}</p>
    * <p>Any subsequent intermediate operations and the terminal operation are then performed locally.</p>
    * @param comparator the comparator to be used for sorting the elements
    * @return the new stream
    */
   @Override
   Stream<R> sorted(Comparator<? super R> comparator);

   /**
    * {@inheritDoc}
    * <p>This intermediate operation will be performed both remotely and locally to reduce how many elements
    * are sent back from each node.  More specifically this operation is applied remotely on each node to only return
    * up to the <b>maxSize</b> value and then the aggregated results are limited once again on the local node.</p>
    * <p>This operation will act as an intermediate iterator operation requiring data be brought locally for proper
    * behavior.  This is described in more detail in the {@link CacheStream} documentation</p>
    * <p>Any subsequent intermediate operations and the terminal operation are then performed locally.</p>
    * @param maxSize how many elements to limit this stream to.
    * @return the new stream
    */
   @Override
   Stream<R> limit(long maxSize);

   /**
    * {@inheritDoc}
    * <p>This operation is performed entirely on the local node irrespective of the backing cache.  This
    * operation will act as an intermediate iterator operation requiring data be brought locally for proper behavior.
    * This is described in more detail in the {@link CacheStream} documentation</p>
    * <p>Depending on the terminal operator this may or may not require all entries or a subset after skip is applied
    * to be in memory all at once.</p>
    * <p>Any subsequent intermediate operations and the terminal operation are then performed locally.</p>
    * @param n how many elements to skip from this stream
    * @return the new stream
    */
   @Override
   Stream<R> skip(long n);

   /**
    * {@inheritDoc}
    * <p>This operation will be invoked both remotely and locally when used with a distributed cache backing this stream.
    * This operation will act as an intermediate iterator operation requiring data be brought locally for proper
    * behavior.  This is described in more detail in the {@link CacheStream} documentation</p>
    * <p>This intermediate iterator operation will be performed locally and remotely requiring possibly a subset of
    * all elements to be in memory</p>
    * <p>Any subsequent intermediate operations and the terminal operation are then performed locally.</p>
    * @return the new stream
    */
   @Override
   Stream<R> distinct();

   /**
    * {@inheritDoc}
    * <p>Note when using a distributed backing cache for this stream the collector must be marshallable.  This
    * prevents the usage of {@link java.util.stream.Collectors} class.  However you can use the
    * {@link org.infinispan.stream.CacheCollectors} static factory methods to create a serializable wrapper, which then
    * creates the actual collector lazily after being deserialized.  This is useful to use any method from the
    * {@link java.util.stream.Collectors} class as you would normally.</p>
    * @param collector
    * @param <R1> collected type
    * @param <A> intermediate collected type if applicable
    * @return the collected value
    * @see org.infinispan.stream.CacheCollectors
    */
   @Override
   <R1, A> R1 collect(Collector<? super R, A, R1> collector);
}
