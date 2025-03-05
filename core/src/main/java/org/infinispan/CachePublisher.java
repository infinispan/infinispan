package org.infinispan;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.StateTransferConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableFunction;
import org.reactivestreams.Publisher;

/**
 * Publisher to be used for non-blocking operations across the cache data set. Note that implementations must use
 * the context of the Cache and the current thread it is invoked on. This means any current invocation context or
 * {@link org.infinispan.context.Flag}s must be adhered to.
 * <p>
 * Note that any {@link Function} passed as an argument to any method on this interface will be marshalled and ran
 * on the remote node. This means any state these Functions may reference will also be marshalled and these functions
 * must not have any side effects.
 * <p>
 * The {@link PublisherReducers} class has many utility methods that can be used as the functions as necessary. Note
 * that any arguments to these methods must also be marshallable still.
 * <p>
 * Note that any processor or data observed from the returned Publisher or Stage will be performed solely in the subscriber's
 * node and thus can use data that cannot be marshalled and may have side effects as needed.
 * @param <K> key type in the cache
 * @param <V> value type in the cache
 * <p>
 * This API is currently Experimental and may be changed or even removed later, use at your own risk.
 */
@Experimental
public interface CachePublisher<K, V> {

   /**
    * Configures if reduction methods should operate in parallel across nodes. This only affects methods
    * like {@link #entryReduction(Function, Function)}, {@link #keyReduction(Function, Function)} and
    * {@link #performRemotely(Function, BiFunction)}.
    * The {@link #entryPublisher(Function)} and {@link #keyPublisher(SerializableFunction)} are not affected
    * by this setting. Defaults to <b>false</b>.
    * <p>
    * Care should be taken when setting this as it may cause adverse performance in some use cases.
    * @param parallel whether the operation performs in parallel across nodes
    * @return a publisher that will perform reduction methods in parallel or not
    */
   CachePublisher<K, V> parallelReduction(boolean parallel);

   /**
    * Configures publisher methods chunk size when retrieving remote values. This only affects methods like
    * {@link #entryPublisher(Function)} and {@link #keyPublisher(Function)}. The
    * {@link #entryReduction(Function, Function)}, {@link #keyReduction(Function, Function)} and
    * {@link #performRemotely(Function, BiFunction)} methods are unaffected. Defaults to the cache's
    * configured {@link StateTransferConfiguration#chunkSize()}.
    * @param batchSize the size of the batch to use when retrieving remote entries
    * @return a publisher that will perform publish methods with the given batch size
    */
   CachePublisher<K, V> batchSize(int batchSize);

   /**
    * Allows callers to configure the publisher to only publish keys or values that map to the given keys in the provided
    * Set. Defaults to null or "all" keys.
    * <p>
    * Note that if this and {@link #filterSegments(IntSet)} are both used, then a key is only returned if it is also maps to
    * a provided segment.
    * @param keys set of keys that should only be used (if null all keys will be evaluated)
    * @return a publisher that will filter published values based on the given keys
    */
   CachePublisher<K, V> filterKeys(Set<? extends K> keys);

   /**
    * Allows callers to configure the publisher to only publish keys or values that map to a given segment in the provided
    * IntSet. The {@link org.infinispan.commons.util.IntSets} is recommended to be used. Defaults to null or "all" segments.
    * <p>
    * Note that if this and {@link #filterKeys(Set)} are both used, then a key is only returned if it is also maps to
    * a provided segment.
    * @param segments determines what entries should be evaluated by only using ones that map to the given segments (if null all segments will be evaluated)
    * @return a publisher that will filter published values based on the given segments
    */
   CachePublisher<K, V> filterSegments(IntSet segments);

   /**
    * Allows the caller to configure what level of consistency is required when retrieving data. Please check each method
    * as they may have implementation specifics for a given guarantee type. Defaults to
    * {@link DeliveryGuarantee#EXACTLY_ONCE} which is the strictest but guarantees data under topology changes
    * @param guarantee the consistency guarantee desired for operations being invoked
    * @return a publisher that will evaluate data under the given data guarantee
    */
   CachePublisher<K, V> deliveryGuarantee(DeliveryGuarantee guarantee);

   /**
    * Same as {@link #entryReduction(Function, Function)}
    * except that the source publisher provided to the <b>transformer</b> is made up of keys only.
    * @param <R> return value type
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<R> keyReduction(Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
                                       Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Same as {@link #keyReduction(Function, Function)} except that the
    * Functions must also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param <R> return value type
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<R> keyReduction(SerializableFunction<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
                                       SerializableFunction<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Performs the given <b>transformer</b> and <b>finalizer</b> on data in the cache, resulting in a single value.
    * Depending on the <b>deliveryGuarantee</b> the <b>transformer</b> may be invoked <b>1..numSegments</b> times. It
    * could be that the <b>transformer</b> is invoked for every segment and produces a result. All of these results
    * are then fed into the <b>finalizer</b> to produce a final result. If publisher is parallel the <b>finalizer</b>
    * will be invoked on each node to ensure there is only a single result per node.
    * <p>
    * If the provided <b>transformer</b> internally uses a reduction with a default value, that value must be its identity value.
    * This is the same as can be seen at {@link java.util.stream.Stream#reduce(Object, BinaryOperator)}.
    * Then as long as the <b>finalizer</b> can handle the identity value it will be properly reduced.
    * @param <R> return value type
    * @param transformer reduces the given publisher of data eventually into a single value. Must not be null.
    * @param finalizer reduces all of the single values produced by the transformer or this finalizer into one final value. Must not be null.
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<R> entryReduction(Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
                                         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Same as {@link #entryReduction(Function, Function)} except that the
    * Functions must also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param transformer
    * @param finalizer
    * @return
    * @param <R>
    */
   <R> CompletionStage<R> entryReduction(SerializableFunction<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
                                         SerializableFunction<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Same as {@link #entryPublisher(Function)}
    * except that the source publisher provided to the <b>transformer</b> is made up of keys only.
    * @param <R> return value type
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    */
   <R> SegmentPublisherSupplier<R> keyPublisher(Function<? super Publisher<K>, ? extends Publisher<R>> transformer);

   /**
    * Same as {@link #keyPublisher(Function)} except that the
    * Function must also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param <R> return value type
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    */
   <R> SegmentPublisherSupplier<R> keyPublisher(SerializableFunction<? super Publisher<K>, ? extends Publisher<R>> transformer);

   /**
    * Performs the given <b>transformer</b> on data in the cache, resulting in multiple values. If a single
    * value is desired, the user should use {@link #entryReduction(Function, Function)}
    * instead as it can optimize some things. Depending on the <b>deliveryGuarantee</b> the <b>transformer</b> may be
    * invoked <b>1..numSegments</b> times per node. Results from a given node will retrieve values up to
    * {@code batchSize} values until some are consumed.
    * <p>
    * For example when using RxJava and using an intermediate operation such as
    * {@link io.reactivex.rxjava3.core.Flowable#switchIfEmpty(Publisher)} this can add elements if the given Publisher
    * is empty, and it is very possible that a segment may not have entries and therefore may add the elements the
    * switched Publisher returns multiple times.
    * <p>
    * Methods that add elements to the returned Publisher are fine as long as they are tied to a specific entry, for
    * example {@link io.reactivex.rxjava3.core.Flowable#flatMap(io.reactivex.rxjava3.functions.Function)} which can reproduce
    * the same elements when provided the same input entry from the cache.
    *
    * @param transformer transform the given stream of data into something else (requires non null)
    * @param <R> return value type
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    */
   <R> SegmentPublisherSupplier<R> entryPublisher(Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer);

   /**
    * Same as {@link #entryPublisher(Function)} except that the
    * Function must also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    * @param transformer
    * @return
    * @param <R>
    */
   <R> SegmentPublisherSupplier<R> entryPublisher(SerializableFunction<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer);

   /**
    * Similar to {@link CacheStream#forEach(SerializableBiConsumer)} in that it allows for the <b>transformer</b> and
    * the <b>consumer</b> to be both ran on remote nodes allowing for lowest latency updates. Note that the operation
    * will usually be ran on the primary owner, but there is no guarantee of this, especially during a rehash. If a rehash
    * occurs the consumer may be ran more than once, so it is highly recommended to ensure these operations are idempotent
    * if possible.
    * @param transformer
    * @param consumer
    * @return
    * @param <R>
    */
   <R> CompletionStage<Void> performRemotely(Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer,
                                             BiFunction<Cache<K, V>, R, CompletionStage<?>> consumer);

   /**
    * Same as {@link #performRemotely(Function, BiFunction)}  except that the
    * Functions must also implement <code>Serializable</code>
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>
    */
   <R> CompletionStage<Void> performRemotely(SerializableFunction<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer,
                                             SerializableBiFunction<Cache<K, V>, R, CompletionStage<?>> consumer);
}
