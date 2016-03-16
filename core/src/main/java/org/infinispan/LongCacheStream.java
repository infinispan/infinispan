package org.infinispan;

import org.infinispan.util.function.SerializableLongBinaryOperator;
import org.infinispan.util.function.SerializableLongConsumer;
import org.infinispan.util.function.SerializableLongFunction;
import org.infinispan.util.function.SerializableLongPredicate;
import org.infinispan.util.function.SerializableLongToDoubleFunction;
import org.infinispan.util.function.SerializableLongToIntFunction;
import org.infinispan.util.function.SerializableLongUnaryOperator;
import org.infinispan.util.function.SerializableObjLongConsumer;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableSupplier;

import java.util.OptionalLong;
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

/**
 * A {@link LongStream} that has additional methods to allow for Serializable instances.  Please see
 * {@link CacheStream} for additional details about various methods.
 *
 * @author wburns
 * @since 9.0
 */
public interface LongCacheStream extends LongStream {
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
   LongCacheStream filter(SerializableLongPredicate predicate);

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
   LongCacheStream map(SerializableLongUnaryOperator mapper);

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
   <U> CacheStream<U> mapToObj(SerializableLongFunction<? extends U> mapper);

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
   IntCacheStream mapToInt(SerializableLongToIntFunction mapper);

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
   DoubleCacheStream mapToDouble(SerializableLongToDoubleFunction mapper);

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
   LongCacheStream flatMap(SerializableLongFunction<? extends LongStream> mapper);

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
   LongCacheStream peek(SerializableLongConsumer action);

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
   void forEach(SerializableLongConsumer action);

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
   <K, V> void forEach(SerializableObjLongConsumer<Cache<K, V>> action);

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
   long reduce(long identity, SerializableLongBinaryOperator op);

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
   OptionalLong reduce(SerializableLongBinaryOperator op);

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
   <R> R collect(SerializableSupplier<R> supplier, SerializableObjLongConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner);

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
   boolean anyMatch(SerializableLongPredicate predicate);

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
   boolean allMatch(SerializableLongPredicate predicate);

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
   boolean noneMatch(SerializableLongPredicate predicate);

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
