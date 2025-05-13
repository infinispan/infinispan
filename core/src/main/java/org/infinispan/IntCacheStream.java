package org.infinispan;

import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableIntBinaryOperator;
import org.infinispan.util.function.SerializableIntConsumer;
import org.infinispan.util.function.SerializableIntFunction;
import org.infinispan.util.function.SerializableIntPredicate;
import org.infinispan.util.function.SerializableIntToDoubleFunction;
import org.infinispan.util.function.SerializableIntToLongFunction;
import org.infinispan.util.function.SerializableIntUnaryOperator;
import org.infinispan.util.function.SerializableObjIntConsumer;
import org.infinispan.util.function.SerializableSupplier;

/**
 * A {@link IntStream} that has additional methods to allow for Serializable instances.  Please see
 * {@link CacheStream} for additional details about various methods.
 *
 * @author wburns
 * @since 9.0
 */
public interface IntCacheStream extends IntStream, BaseCacheStream<Integer, IntStream> {

   /**
    * {@inheritDoc}
    * @return a stream with parallel distribution disabled.
    */
   @Override
   IntCacheStream sequentialDistribution();

   /**
    * {@inheritDoc}
    * @return a stream with parallel distribution enabled.
    */
   @Override
   IntCacheStream parallelDistribution();

   /**
    * {@inheritDoc}
    * @return a stream with the keys filtered.
    */
   @Override
   IntCacheStream filterKeys(Set<?> keys);

   /**
    * {@inheritDoc}
    * @return a stream with the batch size updated
    */
   @Override
   IntCacheStream distributedBatchSize(int batchSize);

   /**
    * {@inheritDoc}
    * @return a stream with the listener registered.
    */
   @Override
   IntCacheStream segmentCompletionListener(SegmentCompletionListener listener);

   /**
    * {@inheritDoc}
    * @return a stream with rehash awareness disabled.
    */
   @Override
   IntCacheStream disableRehashAware();

   /**
    * {@inheritDoc}
    * @return a stream with the timeout set
    */
   @Override
   IntCacheStream timeout(long timeout, TimeUnit unit);

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream filter(IntPredicate predicate);

   /**
    * Same as {@link IntCacheStream#filter(IntPredicate)} except that the IntPredicate must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to each element to determine if it
    *                  should be included
    * @return the new cache int stream
    */
   default IntCacheStream filter(SerializableIntPredicate predicate) {
      return filter((IntPredicate) predicate);
   }

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream map(IntUnaryOperator mapper);

   /**
    * Same as {@link IntCacheStream#map(IntUnaryOperator)} except that the IntUnaryOperator must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache int stream
    */
   default IntCacheStream map(SerializableIntUnaryOperator mapper) {
      return map((IntUnaryOperator) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream
    */
   @Override
   <U> CacheStream<U> mapToObj(IntFunction<? extends U> mapper);

   /**
    * Same as {@link IntCacheStream#mapToObj(IntFunction)} except that the IntFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param <U> the element type of the new stream
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache stream
    */
   default <U> CacheStream<U> mapToObj(SerializableIntFunction<? extends U> mapper) {
      return mapToObj((IntFunction<? extends U>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream mapToDouble(IntToDoubleFunction mapper);

   /**
    * Same as {@link IntCacheStream#mapToDouble(IntToDoubleFunction)} except that the IntToIntFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache double stream
    */
   default DoubleCacheStream mapToDouble(SerializableIntToDoubleFunction mapper) {
      return mapToDouble((IntToDoubleFunction) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream mapToLong(IntToLongFunction mapper);

   /**
    * Same as {@link IntCacheStream#mapToLong(IntToLongFunction)} except that the IntToLongFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache long stream
    */
   default LongCacheStream mapToLong(SerializableIntToLongFunction mapper) {
      return mapToLong((IntToLongFunction) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream flatMap(IntFunction<? extends IntStream> mapper);

   /**
    * Same as {@link IntCacheStream#flatMap(IntFunction)} except that the IntFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element which produces a
    *               {@code IntStream} of new values
    * @return the new cache int stream
    */
   default IntCacheStream flatMap(SerializableIntFunction<? extends IntStream> mapper) {
      return flatMap((IntFunction<? extends IntStream>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream distinct();

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream sorted();

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream peek(IntConsumer action);

   /**
    * Same as {@link IntCacheStream#flatMap(IntFunction)} except that the IntFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param action a non-interfering action to perform on the elements as
    *               they are consumed from the stream
    * @return the new cache int stream
    */
   default IntCacheStream peek(SerializableIntConsumer action) {
      return peek((IntConsumer) action);
   }

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream limit(long maxSize);

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream skip(long n);

   /**
    * Same as {@link IntCacheStream#forEach(IntConsumer)} except that the IntConsumer must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param action a non-interfering action to perform on the elements
    */
   default void forEach(SerializableIntConsumer action) {
      forEach((IntConsumer) action);
   }

   /**
    * Same as {@link IntCacheStream#forEach(IntConsumer)} except that it takes an {@link ObjIntConsumer} that
    * provides access to the underlying {@link Cache} that is backing this stream.
    * <p>
    * Note that the <code>CacheAware</code> interface is not supported for injection using this method as the cache
    * is provided in the consumer directly.
    * @param action consumer to be ran for each element in the stream
    * @param <K> key type of the cache
    * @param <V> value type of the cache
    */
   <K, V> void forEach(ObjIntConsumer<Cache<K, V>> action);

   /**
    * Same as {@link IntCacheStream#forEach(ObjIntConsumer)} except that the <code>BiConsumer</code> must also implement
    * <code>Serializable</code>
    * @param action consumer to be ran for each element in the stream
    * @param <K> key type of the cache
    * @param <V> value type of the cache
    */
   default <K, V> void forEach(SerializableObjIntConsumer<Cache<K, V>> action) {
      forEach((ObjIntConsumer<Cache<K, V>>) action);
   }

   /**
    * Same as {@link IntCacheStream#reduce(int, IntBinaryOperator)} except that the IntBinaryOperator
    * must also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param identity the identity value for the accumulating function
    * @param op an associative, non-interfering, stateless
    *           function for combining two values
    * @return the result of the reduction
    */
   default int reduce(int identity, SerializableIntBinaryOperator op) {
      return reduce(identity, (IntBinaryOperator) op);
   }

   /**
    * Same as {@link IntCacheStream#reduce(IntBinaryOperator)} except that the IntBinaryOperator must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param op an associative, non-interfering, stateless
    *           function for combining two values
    * @return the result of the reduction
    */
   default OptionalInt reduce(SerializableIntBinaryOperator op) {
      return reduce((IntBinaryOperator) op);
   }

   /**
    * Same as {@link IntCacheStream#collect(Supplier, ObjIntConsumer, BiConsumer)} except that the arguments must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param <R> type of the result
    * @param supplier a function that creates a new result container. For a
    *                 parallel execution, this function may be called
    *                 multiple times and must return a fresh value each time.
    * @param accumulator an associative, non-interfering, stateless
    *                    function for incorporating an additional element into a result
    * @param combiner an associative, non-interfering, stateless
    *                    function for combining two values, which must be
    *                    compatible with the accumulator function
    * @return the result of the reduction
    */
   default <R> R collect(SerializableSupplier<R> supplier, SerializableObjIntConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
   }

   /**
    * Same as {@link IntCacheStream#anyMatch(IntPredicate)} except that the IntPredicate must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if any elements of the stream match the provided
    * predicate, otherwise {@code false}
    */
   default boolean anyMatch(SerializableIntPredicate predicate) {
      return anyMatch((IntPredicate) predicate);
   }

   /**
    * Same as {@link IntCacheStream#allMatch(IntPredicate)} except that the IntPredicate must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if either all elements of the stream match the
    * provided predicate or the stream is empty, otherwise {@code false}
    */
   default boolean allMatch(SerializableIntPredicate predicate) {
      return allMatch((IntPredicate) predicate);
   }

   /**
    * Same as {@link IntCacheStream#noneMatch(IntPredicate)} except that the IntPredicate must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if either no elements of the stream match the
    * provided predicate or the stream is empty, otherwise {@code false}
    */
   default boolean noneMatch(SerializableIntPredicate predicate) {
      return noneMatch((IntPredicate) predicate);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream containing integers
    */
   @Override
   CacheStream<Integer> boxed();

   /**
    * {@inheritDoc}
    * @return the cache double stream
    */
   @Override
   DoubleCacheStream asDoubleStream();

   /**
    * {@inheritDoc}
    * @return the cache long stream
    */
   @Override
   LongCacheStream asLongStream();

   /**
    * {@inheritDoc}
    * @return a sequential cache int stream
    */
   @Override
   IntCacheStream sequential();

   /**
    * {@inheritDoc}
    * @return a parallel cache int stream
    */
   @Override
   IntCacheStream parallel();

   /**
    * {@inheritDoc}
    * @return an unordered cache int stream
    */
   @Override
   IntCacheStream unordered();

   /**
    * {@inheritDoc}
    * @return a cache int stream with the handler applied
    */
   @Override
   IntCacheStream onClose(Runnable closeHandler);
}
