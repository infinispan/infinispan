package org.infinispan;

import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableLongBinaryOperator;
import org.infinispan.util.function.SerializableLongConsumer;
import org.infinispan.util.function.SerializableLongFunction;
import org.infinispan.util.function.SerializableLongPredicate;
import org.infinispan.util.function.SerializableLongToDoubleFunction;
import org.infinispan.util.function.SerializableLongToIntFunction;
import org.infinispan.util.function.SerializableLongUnaryOperator;
import org.infinispan.util.function.SerializableObjLongConsumer;
import org.infinispan.util.function.SerializableSupplier;

/**
 * A {@link LongStream} that has additional methods to allow for Serializable instances.  Please see
 * {@link CacheStream} for additional details about various methods.
 *
 * @author wburns
 * @since 9.0
 */
public interface LongCacheStream extends LongStream, BaseCacheStream<Long, LongStream> {

   /**
    * {@inheritDoc}
    * @return a stream with parallel distribution disabled.
    */
   @Override
   LongCacheStream sequentialDistribution();

   /**
    * {@inheritDoc}
    * @return a stream with parallel distribution enabled.
    */
   @Override
   LongCacheStream parallelDistribution();

   /**
    * {@inheritDoc}
    * @return a stream with the keys filtered.
    */
   @Override
   LongCacheStream filterKeys(Set<?> keys);

   /**
    * {@inheritDoc}
    * @return a stream with the batch size updated
    */
   @Override
   LongCacheStream distributedBatchSize(int batchSize);

   /**
    * {@inheritDoc}
    * @return a stream with the listener registered.
    */
   @Override
   LongCacheStream segmentCompletionListener(SegmentCompletionListener listener);

   /**
    * {@inheritDoc}
    * @return a stream with rehash awareness disabled.
    */
   @Override
   LongCacheStream disableRehashAware();

   /**
    * {@inheritDoc}
    * @return a stream with the timeout set
    */
   @Override
   LongCacheStream timeout(long timeout, TimeUnit unit);

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream filter(LongPredicate predicate);

   /**
    * Same as {@link LongCacheStream#filter(LongPredicate)} except that the LongPredicate must also
    * implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to each element to determine if it
    *                  should be included
    * @return the new cache long stream
    */
   default LongCacheStream filter(SerializableLongPredicate predicate) {
      return filter((LongPredicate) predicate);
   }

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream map(LongUnaryOperator mapper);

   /**
    * Same as {@link LongCacheStream#map(LongUnaryOperator)} except that the LongUnaryOperator must also
    * implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache long stream
    */
   default LongCacheStream map(SerializableLongUnaryOperator mapper) {
      return map((LongUnaryOperator) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream
    */
   @Override
   <U> CacheStream<U> mapToObj(LongFunction<? extends U> mapper);

   /**
    * Same as {@link LongCacheStream#mapToObj(LongFunction)} except that the LongFunction must also
    * implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param <U> the element type of the new stream
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache stream
    */
   default <U> CacheStream<U> mapToObj(SerializableLongFunction<? extends U> mapper) {
      return mapToObj((LongFunction<? extends U>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream mapToInt(LongToIntFunction mapper);

   /**
    * Same as {@link LongCacheStream#mapToInt(LongToIntFunction)} except that the LongToIntFunction must also
    * implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache int stream
    */
   default IntCacheStream mapToInt(SerializableLongToIntFunction mapper) {
      return mapToInt((LongToIntFunction) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream mapToDouble(LongToDoubleFunction mapper);

   /**
    * Same as {@link LongCacheStream#mapToDouble(LongToDoubleFunction)} except that the LongToLongFunction must also
    * implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache double stream
    */
   default DoubleCacheStream mapToDouble(SerializableLongToDoubleFunction mapper) {
      return mapToDouble((LongToDoubleFunction) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream flatMap(LongFunction<? extends LongStream> mapper);

   /**
    * Same as {@link LongCacheStream#flatMap(LongFunction)} except that the LongFunction must also
    * implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param mapper a non-interfering, stateless
    *               function to apply to each element which produces a
    *               {@code LongStream} of new values
    * @return the new cache long stream
    */
   default LongCacheStream flatMap(SerializableLongFunction<? extends LongStream> mapper) {
      return flatMap((LongFunction<? extends LongStream>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream distinct();

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream sorted();

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream peek(LongConsumer action);

   /**
    * Same as {@link LongCacheStream#flatMap(LongFunction)} except that the LongFunction must also
    * implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param action a non-interfering action to perform on the elements as
    *               they are consumed from the stream
    * @return the new cache long stream
    */
   default LongCacheStream peek(SerializableLongConsumer action) {
      return peek((LongConsumer) action);
   }

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream limit(long maxSize);

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream skip(long n);

   /**
    * Same as {@link LongCacheStream#forEach(LongConsumer)} except that the LongConsumer must also
    * implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param action a non-interfering action to perform on the elements
    */
   default void forEach(SerializableLongConsumer action) {
      forEach((LongConsumer) action);
   }

   /**
    * Same as {@link LongCacheStream#forEach(LongConsumer)} except that it takes an {@link ObjLongConsumer} that
    * provides access to the underlying {@link Cache} that is backing this stream.
    * <p>
    * Note that the <code>CacheAware</code> interface is not supported for injection using this method as the cache
    * is provided in the consumer directly.
    * @param action consumer to be ran for each element in the stream
    * @param <K> key type of the cache
    * @param <V> value type of the cache
    */
   <K, V> void forEach(ObjLongConsumer<Cache<K, V>> action);

   /**
    * Same as {@link LongCacheStream#forEach(ObjLongConsumer)} except that the <code>BiConsumer</code> must also implement
    * <code>Serializable</code>
    * @param action consumer to be ran for each element in the stream
    * @param <K> key type of the cache
    * @param <V> value type of the cache
    */
   default <K, V> void forEach(SerializableObjLongConsumer<Cache<K, V>> action) {
      forEach((ObjLongConsumer<Cache<K, V>>) action);
   }

   /**
    * Same as {@link LongCacheStream#reduce(long, LongBinaryOperator)} except that the LongBinaryOperator must
    * also implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param identity the identity value for the accumulating function
    * @param op an associative, non-interfering, stateless
    *           function for combining two values
    * @return the result of the reduction
    */
   default long reduce(long identity, SerializableLongBinaryOperator op) {
      return reduce(identity, (LongBinaryOperator) op);
   }

   /**
    * Same as {@link LongCacheStream#reduce(LongBinaryOperator)} except that the LongBinaryOperator must
    * also implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param op an associative, non-interfering, stateless
    *           function for combining two values
    * @return the result of the reduction
    */
   default OptionalLong reduce(SerializableLongBinaryOperator op) {
      return reduce((LongBinaryOperator) op);
   }

   /**
    * Same as {@link LongCacheStream#collect(Supplier, ObjLongConsumer, BiConsumer)} except that the arguments must
    * also implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
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
   default <R> R collect(SerializableSupplier<R> supplier, SerializableObjLongConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
   }

   /**
    * Same as {@link LongCacheStream#anyMatch(LongPredicate)} except that the LongPredicate must
    * also implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if any elements of the stream match the provided
    * predicate, otherwise {@code false}
    */
   default boolean anyMatch(SerializableLongPredicate predicate) {
      return anyMatch((LongPredicate) predicate);
   }

   /**
    * Same as {@link LongCacheStream#allMatch(LongPredicate)} except that the LongPredicate must
    * also implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if either all elements of the stream match the
    * provided predicate or the stream is empty, otherwise {@code false}
    */
   default boolean allMatch(SerializableLongPredicate predicate) {
      return allMatch((LongPredicate) predicate);
   }

   /**
    * Same as {@link LongCacheStream#noneMatch(LongPredicate)} except that the LongPredicate must
    * also implement Serializable.
    * <p>
    * This method will be used automatically by lambdas, which prevents users from having to manually cast to
    * a Serializable lambda.
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if either no elements of the stream match the
    * provided predicate or the stream is empty, otherwise {@code false}
    */
   default boolean noneMatch(SerializableLongPredicate predicate) {
      return noneMatch((LongPredicate) predicate);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream containing longs
    */
   @Override
   CacheStream<Long> boxed();

   /**
    * {@inheritDoc}
    * @return the cache double stream
    */
   @Override
   DoubleCacheStream asDoubleStream();

   /**
    * {@inheritDoc}
    * @return a sequential cache long stream
    */
   @Override
   LongCacheStream sequential();

   /**
    * {@inheritDoc}
    * @return a parallel cache long stream
    */
   @Override
   LongCacheStream parallel();

   /**
    * {@inheritDoc}
    * @return an unordered cache long stream
    */
   @Override
   LongCacheStream unordered();

   /**
    * {@inheritDoc}
    * @return a cache long stream with the handler applied
    */
   @Override
   LongCacheStream onClose(Runnable closeHandler);
}
