package org.infinispan;

import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;

import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableDoubleBinaryOperator;
import org.infinispan.util.function.SerializableDoubleConsumer;
import org.infinispan.util.function.SerializableDoubleFunction;
import org.infinispan.util.function.SerializableDoublePredicate;
import org.infinispan.util.function.SerializableDoubleToIntFunction;
import org.infinispan.util.function.SerializableDoubleToLongFunction;
import org.infinispan.util.function.SerializableDoubleUnaryOperator;
import org.infinispan.util.function.SerializableObjDoubleConsumer;
import org.infinispan.util.function.SerializableSupplier;

/**
 * A {@link DoubleStream} that has additional methods to allow for Serializable instances.  Please see
 * {@link CacheStream} for additional details about various methods.
 *
 * @author wburns
 * @since 9.0
 */
public interface DoubleCacheStream extends DoubleStream, BaseCacheStream<Double, DoubleStream> {

   /**
    * {@inheritDoc}
    * @return a stream with parallel distribution disabled.
    */
   @Override
   DoubleCacheStream sequentialDistribution();

   /**
    * {@inheritDoc}
    * @return a stream with parallel distribution enabled.
    */
   @Override
   DoubleCacheStream parallelDistribution();

   /**
    * {@inheritDoc}
    * @return a stream with the keys filtered.
    */
   @Override
   DoubleCacheStream filterKeys(Set<?> keys);

   /**
    * {@inheritDoc}
    * @return a stream with the batch size updated
    */
   @Override
   DoubleCacheStream distributedBatchSize(int batchSize);

   /**
    * {@inheritDoc}
    * @return a stream with the listener registered.
    */
   @Override
   DoubleCacheStream segmentCompletionListener(SegmentCompletionListener listener);

   /**
    * {@inheritDoc}
    * @return a stream with rehash awareness disabled.
    */
   @Override
   DoubleCacheStream disableRehashAware();

   /**
    * {@inheritDoc}
    * @return a stream with the timeout set
    */
   @Override
   DoubleCacheStream timeout(long timeout, TimeUnit unit);

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream filter(DoublePredicate predicate);

   /**
    * Same as {@link DoubleCacheStream#filter(DoublePredicate)} except that the DoublePredicate must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to each element to determine if it
    *                  should be included
    * @return the new cache double stream
    */
   default DoubleCacheStream filter(SerializableDoublePredicate predicate) {
      return filter((DoublePredicate) predicate);
   }

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream map(DoubleUnaryOperator mapper);

   /**
    * Same as {@link DoubleCacheStream#map(DoubleUnaryOperator)} except that the DoubleUnaryOperator must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache double stream
    */
   default DoubleCacheStream map(SerializableDoubleUnaryOperator mapper) {
      return map((DoubleUnaryOperator) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream
    */
   @Override
   <U> CacheStream<U> mapToObj(DoubleFunction<? extends U> mapper);

   /**
    * Same as {@link DoubleCacheStream#mapToObj(DoubleFunction)} except that the DoubleFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param <U> the element type of the new stream
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache stream
    */
   default <U> CacheStream<U> mapToObj(SerializableDoubleFunction<? extends U> mapper) {
      return mapToObj((DoubleFunction<? extends U>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache int stream
    */
   @Override
   IntCacheStream mapToInt(DoubleToIntFunction mapper);

   /**
    * Same as {@link DoubleCacheStream#mapToInt(DoubleToIntFunction)} except that the DoubleToIntFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache int stream
    */
   default IntCacheStream mapToInt(SerializableDoubleToIntFunction mapper) {
      return mapToInt((DoubleToIntFunction) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache long stream
    */
   @Override
   LongCacheStream mapToLong(DoubleToLongFunction mapper);

   /**
    * Same as {@link DoubleCacheStream#mapToLong(DoubleToLongFunction)} except that the DoubleToLongFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element
    * @return the new cache long stream
    */
   default LongCacheStream mapToLong(SerializableDoubleToLongFunction mapper) {
      return mapToLong((DoubleToLongFunction) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream flatMap(DoubleFunction<? extends DoubleStream> mapper);

   /**
    * Same as {@link DoubleCacheStream#flatMap(DoubleFunction)} except that the DoubleFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param mapper a non-interfering, stateless
    *               function to apply to each element which produces a
    *               {@code DoubleStream} of new values
    * @return the new cache double stream
    */
   default DoubleCacheStream flatMap(SerializableDoubleFunction<? extends DoubleStream> mapper) {
      return flatMap((DoubleFunction<? extends DoubleStream>) mapper);
   }

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream distinct();

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream sorted();

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream peek(DoubleConsumer action);

   /**
    * Same as {@link DoubleCacheStream#flatMap(DoubleFunction)} except that the DoubleFunction must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param action a non-interfering action to perform on the elements as
    *               they are consumed from the stream
    * @return the new cache double stream
    */
   default DoubleCacheStream peek(SerializableDoubleConsumer action) {
      return peek((DoubleConsumer) action);
   }

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream limit(long maxSize);

   /**
    * {@inheritDoc}
    * @return the new cache double stream
    */
   @Override
   DoubleCacheStream skip(long n);

   /**
    * Same as {@link DoubleCacheStream#forEach(DoubleConsumer)} except that the DoubleConsumer must also
    * implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param action a non-interfering action to perform on the elements
    */
   default void forEach(SerializableDoubleConsumer action) {
      forEach((DoubleConsumer) action);
   }

   /**
    * Same as {@link DoubleCacheStream#forEach(DoubleConsumer)} except that it takes an {@link ObjDoubleConsumer} that
    * provides access to the underlying {@link Cache} that is backing this stream.
    * <p>
    * Note that the <code>CacheAware</code> interface is not supported for injection using this method as the cache
    * is provided in the consumer directly.
    * @param action consumer to be ran for each element in the stream
    * @param <K> key type of the cache
    * @param <V> value type of the cache
    */
   <K, V> void forEach(ObjDoubleConsumer<Cache<K, V>> action);

   /**
    * Same as {@link DoubleCacheStream#forEach(ObjDoubleConsumer)} except that the <code>BiConsumer</code> must also implement
    * <code>Serializable</code>
    * @param action consumer to be ran for each element in the stream
    * @param <K> key type of the cache
    * @param <V> value type of the cache
    */
   default <K, V> void forEach(SerializableObjDoubleConsumer<Cache<K, V>> action) {
      forEach((ObjDoubleConsumer<Cache<K, V>>) action);
   }

   /**
    * Same as {@link DoubleCacheStream#reduce(double, DoubleBinaryOperator)} except that the DoubleBinaryOperator must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param identity the identity value for the accumulating function
    * @param op an associative, non-interfering, stateless
    *           function for combining two values
    * @return the result of the reduction
    */
   default double reduce(double identity, SerializableDoubleBinaryOperator op) {
      return reduce(identity, (DoubleBinaryOperator) op);
   }

   /**
    * Same as {@link DoubleCacheStream#reduce(DoubleBinaryOperator)} except that the DoubleBinaryOperator must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param op an associative, non-interfering, stateless
    *           function for combining two values
    * @return the result of the reduction
    */
   default OptionalDouble reduce(SerializableDoubleBinaryOperator op) {
      return reduce((DoubleBinaryOperator) op);
   }

   /**
    * Same as {@link DoubleCacheStream#collect(Supplier, ObjDoubleConsumer, BiConsumer)} except that the arguments must
    * also implement <code>Serializable</code>
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
   default <R> R collect(SerializableSupplier<R> supplier, SerializableObjDoubleConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
   }

   /**
    * Same as {@link DoubleCacheStream#anyMatch(DoublePredicate)} except that the DoublePredicate must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if any elements of the stream match the provided
    * predicate, otherwise {@code false}
    */
   default boolean anyMatch(SerializableDoublePredicate predicate) {
      return anyMatch((DoublePredicate) predicate);
   }

   /**
    * Same as {@link DoubleCacheStream#allMatch(DoublePredicate)} except that the DoublePredicate must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if either all elements of the stream match the
    * provided predicate or the stream is empty, otherwise {@code false}
    */
   default boolean allMatch(SerializableDoublePredicate predicate) {
      return allMatch((DoublePredicate) predicate);
   }

   /**
    * Same as {@link DoubleCacheStream#noneMatch(DoublePredicate)} except that the DoublePredicate must
    * also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param predicate a non-interfering, stateless
    *                  predicate to apply to elements of this stream
    * @return {@code true} if either no elements of the stream match the
    * provided predicate or the stream is empty, otherwise {@code false}
    */
   default boolean noneMatch(SerializableDoublePredicate predicate) {
      return noneMatch((DoublePredicate) predicate);
   }

   /**
    * {@inheritDoc}
    * @return the new cache stream containing doubles
    */
   @Override
   CacheStream<Double> boxed();

   /**
    * {@inheritDoc}
    * @return a sequential cache double stream
    */
   @Override
   DoubleCacheStream sequential();

   /**
    * {@inheritDoc}
    * @return a parallel cache double stream
    */
   @Override
   DoubleCacheStream parallel();

   /**
    * {@inheritDoc}
    * @return an unordered cache double stream
    */
   @Override
   DoubleCacheStream unordered();

   /**
    * {@inheritDoc}
    * @return a cache double stream with the handler applied
    */
   @Override
   DoubleCacheStream onClose(Runnable closeHandler);
}
