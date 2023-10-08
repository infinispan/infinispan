package org.infinispan;

import static org.infinispan.util.Casting.toSerialSupplierCollect;
import static org.infinispan.util.Casting.toSupplierCollect;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.commons.util.IntSet;
import org.infinispan.stream.CacheCollectors;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableBinaryOperator;
import org.infinispan.util.function.SerializableComparator;
import org.infinispan.util.function.SerializableConsumer;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.function.SerializableIntFunction;
import org.infinispan.util.function.SerializablePredicate;
import org.infinispan.util.function.SerializableSupplier;
import org.infinispan.util.function.SerializableToDoubleFunction;
import org.infinispan.util.function.SerializableToIntFunction;
import org.infinispan.util.function.SerializableToLongFunction;

/**
 * A {@link Stream} that has additional operations to monitor or control behavior when used from a {@link Cache}.
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
 * <p>Parallel distribution is enabled by default for all operations except for {@link CacheStream#iterator()} and
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
public interface CacheStream<R> extends Stream<R>, BaseCacheStream<R, Stream<R>> {
   /**
    * {@inheritDoc}
    * @return a stream with parallel distribution disabled.
    */
   CacheStream<R> sequentialDistribution();

   /**
    * @inheritDoc
    * @return a stream with parallel distribution enabled.
    */
   CacheStream<R> parallelDistribution();

   /**
    * {@inheritDoc}
    * @return a stream with the segments filtered.
    */
   CacheStream<R> filterKeySegments(IntSet segments);

   /**
    * {@inheritDoc}
    * @return a stream with the keys filtered.
    */
   CacheStream<R> filterKeys(Set<?> keys);

   /**
    * {@inheritDoc}
    * @return a stream with the batch size updated
    */
   CacheStream<R> distributedBatchSize(int batchSize);

   /**
    * {@inheritDoc}
    * @return a stream with the listener registered.
    */
   CacheStream<R> segmentCompletionListener(SegmentCompletionListener listener);

   /**
    * {@inheritDoc}
    * @return a stream with rehash awareness disabled.
    */
   CacheStream<R> disableRehashAware();

   /**
    * {@inheritDoc}
    * @return a stream with the timeout set
    */
   CacheStream<R> timeout(long timeout, TimeUnit unit);

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
    * @param action consumer to be ran for each element in the stream
    */
   @Override
   void forEach(Consumer<? super R> action);

   /**
    * Same as {@link CacheStream#forEach(Consumer)} except that the Consumer must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param action consumer to be ran for each element in the stream
    */
   default void forEach(SerializableConsumer<? super R> action) {
      forEach((Consumer<? super R>) action);
   }

   /**
    * Same as {@link CacheStream#forEach(Consumer)} except that it takes a {@link BiConsumer} that provides access
    * to the underlying {@link Cache} that is backing this stream.
    * <p>
    * Note that the <code>CacheAware</code> interface is not supported for injection using this method as the cache
    * is provided in the consumer directly.
    * @param action consumer to be ran for each element in the stream
    * @param <K> key type of the cache
    * @param <V> value type of the cache
    */
   <K, V> void forEach(BiConsumer<Cache<K, V>, ? super R> action);

   /**
    * Same as {@link CacheStream#forEach(BiConsumer)} except that the <code>BiConsumer</code> must also implement
    * <code>Serializable</code>
    * @param action consumer to be ran for each element in the stream
    * @param <K> key type of the cache
    * @param <V> value type of the cache
    */
   default <K, V> void forEach(SerializableBiConsumer<Cache<K, V>, ? super R> action) {
      forEach((BiConsumer<Cache<K, V>, ? super R>) action);
   }

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
   CacheStream<R> sorted();

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
   CacheStream<R> sorted(Comparator<? super R> comparator);

   /**
    * Same as {@link CacheStream#sorted(Comparator)} except that the Comparator must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param comparator a non-interfering, stateless
    *                   {@code Comparator} to be used to compare stream elements
    * @return the new stream
    */
   default CacheStream<R> sorted(SerializableComparator<? super R> comparator) {
      return sorted((Comparator<? super R>) comparator);
   }

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
   CacheStream<R> limit(long maxSize);

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
   CacheStream<R> skip(long n);

   /**
    * {@inheritDoc}
    * @param  action the action to perform on the stream
    * @return the new stream
    */
   @Override
   CacheStream<R> peek(Consumer<? super R> action);

   /**
    * Same as {@link CacheStream#peek(Consumer)} except that the Consumer must also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param action a non-interfering action to perform on the elements as
    *                 they are consumed from the stream
    * @return the new stream
    */
   default CacheStream<R> peek(SerializableConsumer<? super R> action) {
      return peek((Consumer<? super R>) action);
   }

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
   CacheStream<R> distinct();

   /**
    * {@inheritDoc}
    * <p>Note when using a distributed backing cache for this stream the collector must be marshallable.  This
    * prevents the usage of {@link java.util.stream.Collectors} class.  However you can use the
    * {@link org.infinispan.stream.CacheCollectors} static factory methods to create a serializable wrapper, which then
    * creates the actual collector lazily after being deserialized.  This is useful to use any method from the
    * {@link java.util.stream.Collectors} class as you would normally.
    * Alternatively, you can call {@link #collect(SerializableSupplier)} too.</p>
    *
    * <p>Note: The collector is applied on each node until all the local stream's values
    * are reduced into a single object.
    * Because of marshalling limitations, the final result of the collector on remote nodes
    * is limited to a size of 2GB.
    * If you need to process more than 2GB of data, you must force the collector to run on the
    * originator with {@link #spliterator()}:
    * <pre>
    * StreamSupport.stream(stream.filter(entry -> ...)
    *                            .map(entry -> ...)
    *                            .spliterator(),
    *                      false)
    *              .collect(Collectors.toList());
    * </pre>
    * </p>
    *
    * @param collector
    * @param <R1> collected type
    * @param <A> intermediate collected type if applicable
    * @return the collected value
    * @see org.infinispan.stream.CacheCollectors
    */
   @Override
   <R1, A> R1 collect(Collector<? super R, A, R1> collector);

   /**
    * Performs a <a href="package-summary.html#MutableReduction">mutable
    * reduction</a> operation on the elements of this stream using a
    * {@code Collector} that is lazily created from the {@code SerializableSupplier}
    * provided.
    *
    * This method behaves exactly the same as {@link #collect(Collector)} with
    * the enhanced capability of working even when the mutable reduction
    * operation has to run in a remote node and the operation is not
    * {@link Serializable} or otherwise marshallable.
    *
    * So, this method is specially designed for situations when the user
    * wants to use a {@link Collector} instance that has been created by
    * {@link java.util.stream.Collectors} static factory methods.
    *
    * In this particular case, the function that instantiates the
    * {@link Collector} will be marshalled according to the
    * {@link Serializable} rules.
    *
    * <p>Note: The collector is applied on each node until all the local stream's values
    * are reduced into a single object.
    * Because of marshalling limitations, the final result of the collector on remote nodes
    * is limited to a size of 2GB.
    * If you need to process more than 2GB of data, you must force the collector to run on the
    * originator with {@link #spliterator()}:
    * <pre>
    * StreamSupport.stream(stream.filter(entry -> ...)
    *                            .map(entry -> ...)
    *                            .spliterator(),
    *                      false)
    *              .collect(Collectors.toList());
    * </pre>
    * </p>
    *
    * @param supplier The supplier to create the collector that is specifically serializable
    * @param <R1> The resulting type of the collector
    * @return the collected value
    * @since 9.2
    */
   default <R1> R1 collect(SerializableSupplier<Collector<? super R, ?, R1>> supplier) {
      return collect(CacheCollectors.serializableCollector(toSerialSupplierCollect(supplier)));
   }

   /**
    * Performs a <a href="package-summary.html#MutableReduction">mutable
    * reduction</a> operation on the elements of this stream using a
    * {@code Collector} that is lazily created from the {@code Supplier}
    * provided.
    *
    * This method behaves exactly the same as {@link #collect(Collector)} with
    * the enhanced capability of working even when the mutable reduction
    * operation has to run in a remote node and the operation is not
    * {@link Serializable} or otherwise marshallable.
    *
    * So, this method is specially designed for situations when the user
    * wants to use a {@link Collector} instance that has been created by
    * {@link java.util.stream.Collectors} static factory methods.
    *
    * In this particular case, the function that instantiates the
    * {@link Collector} will be marshalled using the Infinispan marshaller.
    *
    * <p>Note: The collector is applied on each node until all the local stream's values
    * are reduced into a single object.
    * Because of marshalling limitations, the final result of the collector on remote nodes
    * is limited to a size of 2GB.
    * If you need to process more than 2GB of data, you must force the collector to run on the
    * originator with {@link #spliterator()}:
    * <pre>
    * StreamSupport.stream(stream.filter(entry -> ...)
    *                            .map(entry -> ...)
    *                            .spliterator(),
    *                      false)
    *              .collect(Collectors.toList());
    * </pre>
    * </p>
    *
    * @param supplier The supplier to create the collector
    * @param <R1> The resulting type of the collector
    * @return the collected value
    * @since 9.2
    */
   default <R1> R1 collect(Supplier<Collector<? super R, ?, R1>> supplier) {
      return collect(CacheCollectors.collector(toSupplierCollect(supplier)));
   }

   /**
    * Same as {@link CacheStream#collect(Supplier, BiConsumer, BiConsumer)} except that the various arguments must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    *
    * @param <R1> type of the result
    * @param supplier a function that creates a new result container. For a
    *                 parallel execution, this function may be called
    *                 multiple times and must return a fresh value each time.
    *                 Must be serializable
    * @param accumulator an associative, non-interfering, stateless
    *                    function for incorporating an additional element into a result and
    *                    must be serializable
    * @param combiner an associative, non-interfering, stateless
    *                    function for combining two values, which must be
    *                    compatible with the accumulator function and serializable
    * @return the result of the reduction
    */
   default <R1> R1 collect(SerializableSupplier<R1> supplier, SerializableBiConsumer<R1, ? super R> accumulator,
           SerializableBiConsumer<R1, R1> combiner) {
      return collect((Supplier<R1>) supplier, accumulator, combiner);
   }

   /**
    * {@inheritDoc}
    *
    * <p>Note: The accumulator and combiner are applied on each node until all the local stream's values
    * are reduced into a single object.
    * Because of marshalling limitations, the final result of the collector on remote nodes
    * is limited to a size of 2GB.
    * If you need to process more than 2GB of data, you must force the collector to run on the
    * originator with {@link #spliterator()}:
    * <pre>
    * StreamSupport.stream(stream.filter(entry -> ...)
    *                            .map(entry -> ...)
    *                            .spliterator(),
    *                      false)
    *              .collect(Collectors.toList());
    * </pre>
    * </p>
    */
   @Override
   <R1> R1 collect(Supplier<R1> supplier, BiConsumer<R1, ? super R> accumulator, BiConsumer<R1, R1> combiner);

   /**
    * Same as {@link CacheStream#allMatch(Predicate)} except that the Predicate must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream that is serializable
    * @return {@code true} if either all elements of the stream match the
    * provided predicate or the stream is empty, otherwise {@code false}
    */
   default boolean allMatch(SerializablePredicate<? super R> predicate) {
      return allMatch((Predicate<? super R>) predicate);
   }

   /**
    * Same as {@link CacheStream#noneMatch(Predicate)} except that the Predicate must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream that is serializable
    * @return {@code true} if either no elements of the stream match the
    * provided predicate or the stream is empty, otherwise {@code false}
    */
   default boolean noneMatch(SerializablePredicate<? super R> predicate) {
      return noneMatch((Predicate<? super R>) predicate);
   }

   /**
    * Same as {@link CacheStream#anyMatch(Predicate)} except that the Predicate must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream that is serializable
    * @return {@code true} if any elements of the stream match the provided
    * predicate, otherwise {@code false}
    */
   default boolean anyMatch(SerializablePredicate<? super R> predicate) {
      return anyMatch((Predicate<? super R>) predicate);
   }

   /**
    * Same as {@link CacheStream#max(Comparator)} except that the Comparator must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param comparator a non-interfering, stateless
    *                   {@code Comparator} to compare elements of this stream that is also serializable
    * @return an {@code Optional} describing the maximum element of this stream,
    * or an empty {@code Optional} if the stream is empty
    */
   default Optional<R> max(SerializableComparator<? super R> comparator) {
      return max((Comparator<? super R>) comparator);
   }

   /**
    * Same as {@link CacheStream#min(Comparator)} except that the Comparator must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param comparator a non-interfering, stateless
    *                   {@code Comparator} to compare elements of this stream that is also serializable
    * @return an {@code Optional} describing the minimum element of this stream,
    * or an empty {@code Optional} if the stream is empty
    */
   default Optional<R> min(SerializableComparator<? super R> comparator) {
      return min((Comparator<? super R>) comparator);
   }

   /**
    * Same as {@link CacheStream#reduce(BinaryOperator)} except that the BinaryOperator must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param accumulator an associative, non-interfering, stateless
    *                    function for combining two values that is also serializable
    * @return an {@link Optional} describing the result of the reduction
    */
   default Optional<R> reduce(SerializableBinaryOperator<R> accumulator) {
      return reduce((BinaryOperator<R>) accumulator);
   }

   /**
    * Same as {@link CacheStream#reduce(Object, BinaryOperator)}  except that the BinaryOperator must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param identity the identity value for the accumulating function
    * @param accumulator an associative, non-interfering, stateless
    *                    function for combining two values that is also serializable
    * @return the result of the reduction
    */
   default R reduce(R identity, SerializableBinaryOperator<R> accumulator) {
      return reduce(identity, (BinaryOperator<R>) accumulator);
   }

   /**
    * Same as {@link CacheStream#reduce(Object, BiFunction, BinaryOperator)} except that the BinaryOperator must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    *
    * <p>Just like in the cache, {@code null} values are not supported.</p>
    *
    * @param <U> The type of the result
    * @param identity the identity value for the combiner function
    * @param accumulator an associative, non-interfering, stateless
    *                    function for incorporating an additional element into a result  that is also serializable
    * @param combiner an associative, non-interfering, stateless
    *                    function for combining two values, which must be
    *                    compatible with the accumulator function that is also serializable
    * @return the result of the reduction
    */
   default <U> U reduce(U identity, SerializableBiFunction<U, ? super R, U> accumulator,
         SerializableBinaryOperator<U> combiner) {
      return reduce(identity, (BiFunction<U, ? super R, U>) accumulator, combiner);
   }

   /**
    * Same as {@link CacheStream#toArray(IntFunction)} except that the BinaryOperator must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param <A> the element type of the resulting array
    * @param generator a function which produces a new array of the desired
    *                  type and the provided length that is also serializable
    * @return an array containing the elements in this stream
    */
   default <A> A[] toArray(SerializableIntFunction<A[]> generator) {
      return toArray((IntFunction<A[]>) generator);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream
    */
   @Override
   CacheStream<R> filter(Predicate<? super R> predicate);

   /**
    * Same as {@link CacheStream#filter(Predicate)} except that the Predicate must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to each element to determine if it
    *                  should be included
    * @return the new cache stream
    */
   default CacheStream<R> filter(SerializablePredicate<? super R> predicate) {
      return filter((Predicate<? super R>) predicate);
   }

   /**
    * {@inheritDoc}
    *
    * <p>Just like in the cache, {@code null} values are not supported.</p>
    *
    * @return the new cache stream
    */
   @Override
   <R1> CacheStream<R1> map(Function<? super R, ? extends R1> mapper);

   /**
    * Same as {@link CacheStream#map(Function)} except that the Function must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param <R1> The element type of the new stream
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache stream
    */
   default <R1> CacheStream<R1> map(SerializableFunction<? super R, ? extends R1> mapper) {
      return map((Function<? super R, ? extends R1>) mapper);
   }

   /**
    * {@inheritDoc}
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new double cache stream
    */
   @Override
   DoubleCacheStream mapToDouble(ToDoubleFunction<? super R> mapper);

   /**
    * Same as {@link CacheStream#mapToDouble(ToDoubleFunction)}  except that the ToDoubleFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new stream
    */
   default DoubleCacheStream mapToDouble(SerializableToDoubleFunction<? super R> mapper) {
      return mapToDouble((ToDoubleFunction<? super R>) mapper);
   }

   /**
    * {@inheritDoc}
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new int cache stream
    */
   @Override
   IntCacheStream mapToInt(ToIntFunction<? super R> mapper);

   /**
    * Same as {@link CacheStream#mapToInt(ToIntFunction)}  except that the ToIntFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new stream
    */
   default IntCacheStream mapToInt(SerializableToIntFunction<? super R> mapper) {
      return mapToInt((ToIntFunction<? super R>) mapper);
   }

   /**
    * {@inheritDoc}
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new long cache stream
    */
   @Override
   LongCacheStream mapToLong(ToLongFunction<? super R> mapper);

   /**
    * Same as {@link CacheStream#mapToLong(ToLongFunction)}  except that the ToLongFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new stream
    */
   default LongCacheStream mapToLong(SerializableToLongFunction<? super R> mapper) {
      return mapToLong((ToLongFunction<? super R>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream
    */
   @Override
   <R1> CacheStream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper);

   /**
    * Same as {@link CacheStream#flatMap(Function)} except that the Function must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param <R1> The element type of the new stream
    * @param mapper a non-interfering, stateless
    *               function to apply to each element which produces a stream
    *               of new values
    * @return the new cache stream
    */
   default <R1> CacheStream<R1> flatMap(SerializableFunction<? super R, ? extends Stream<? extends R1>> mapper) {
      return flatMap((Function<? super R, ? extends Stream<? extends R1>>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream
    */
   @Override
   DoubleCacheStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper);

   /**
    * Same as {@link CacheStream#flatMapToDouble(Function)} except that the Function must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element which produces a stream
    *               of new values
    * @return the new stream
    */
   default DoubleCacheStream flatMapToDouble(SerializableFunction<? super R, ? extends DoubleStream> mapper) {
      return flatMapToDouble((Function<? super R, ? extends DoubleStream>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream
    */
   @Override
   IntCacheStream flatMapToInt(Function<? super R, ? extends IntStream> mapper);

   /**
    * Same as {@link CacheStream#flatMapToInt(Function)} except that the Function must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element which produces a stream
    *               of new values
    * @return the new stream
    */
   default IntCacheStream flatMapToInt(SerializableFunction<? super R, ? extends IntStream> mapper) {
      return flatMapToInt((Function<? super R, ? extends IntStream>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream
    */
   @Override
   LongCacheStream flatMapToLong(Function<? super R, ? extends LongStream> mapper);

   /**
    * Same as {@link CacheStream#flatMapToLong(Function)} except that the Function must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element which produces a stream
    *               of new values
    * @return the new stream
    */
   default LongCacheStream flatMapToLong(SerializableFunction<? super R, ? extends LongStream> mapper) {
      return flatMapToLong((Function<? super R, ? extends LongStream>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return a parallel cache stream
    */
   @Override
   CacheStream<R> parallel();

   /**
    * {@inheritDoc}
    * @return a sequential cache stream
    */
   @Override
   CacheStream<R> sequential();

   /**
    * {@inheritDoc}
    * @return an unordered cache stream
    */
   @Override
   CacheStream<R> unordered();

   /**
    * {@inheritDoc}
    * @param closeHandler
    * @return a cache stream with the handler applied
    */
   @Override
   CacheStream<R> onClose(Runnable closeHandler);
}
