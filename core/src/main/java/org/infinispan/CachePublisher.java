package org.infinispan;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.StateTransferConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.reactive.publisher.PublisherTransformers;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.util.function.SerializableFunction;
import org.reactivestreams.Publisher;

/**
 * Publisher to be used for non-blocking operations across the cache data set. Note that implementations must use
 * the context of the Cache and the current thread it is invoked on. This means any current invocation context or
 * {@link org.infinispan.context.Flag}s must be adhered to.
 * <p>
 * A CachePublisher is immutable and any method that returns a CachePublisher may return a new instance.
 * <p>
 * Note that any {@link Function} passed as an argument to any method on this interface will be marshalled and ran
 * on the remote node. This means any state these Functions may reference will also be marshalled and these functions
 * must not have any side effects.
 * <p>
 * The {@link PublisherReducers} and {@link PublisherTransformers} classes have utility methods that can be used as
 * functions as necessary. Note that any arguments to these methods must also be
 * marshallable still.
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
    * Configures reduction methods to operate in parallel across nodes. This only affects methods
    * like {@link #entryReduction(Function, Function)} and {@link #keyReduction(Function, Function)}.
    * The {@link #entryPublisher(Function)} and {@link #keyPublisher(SerializableFunction)} are not affected
    * by this setting. Defaults to <b>sequential</b>.
    * <p>
    * Care should be taken when setting this as it may cause adverse performance in some use cases.
    * @return a publisher that will perform reduction methods in parallel
    */
   CachePublisher<K, V> parallelReduction();

   /**
    * Configures reduction methods to operate in sequential across nodes. This only affects methods
    * like {@link #entryReduction(Function, Function)} and {@link #keyReduction(Function, Function)}.
    * The {@link #entryPublisher(Function)} and {@link #keyPublisher(SerializableFunction)} are not affected
    * by this setting. Defaults to <b>sequential</b>.
    * @return a publisher that will perform reduction methods in sequential
    */
   CachePublisher<K, V> sequentialReduction();

   /**
    * Configures publisher methods chunk size when retrieving remote values. This only affects methods like
    * {@link #entryPublisher(Function)} and {@link #keyPublisher(Function)}. The
    * {@link #entryReduction(Function, Function)} and {@link #keyReduction(Function, Function)} methods are unaffected.
    * Defaults to the cache's configured {@link StateTransferConfiguration#chunkSize()}.
    * @param batchSize the size of the batch to use when retrieving remote entries
    * @return a publisher that will perform publish methods with the given batch size
    * @throws IllegalArgumentException if the provided value is 0 or less
    */
   CachePublisher<K, V> batchSize(int batchSize);

   /**
    * Configures the publisher to only publish keys or values that map to the given keys in the provided
    * Set. Defaults to "all" keys, which can be done by invoking {@link #withAllKeys()}.
    * <p>
    * Note that if this and {@link #withSegments(IntSet)} are both used, then a key is only returned if it is also maps to
    * a provided segment.
    * @param keys set of keys that should only be used
    * @return a publisher that will filter published values based on the given keys
    * @throws NullPointerException if the provided Set is null
    */
   CachePublisher<K, V> withKeys(Set<? extends K> keys);

   /**
    * Configures a publisher to publish all keys or values, overriding if {@link #withKeys(Set)} was invoked.
    * @return a publisher that will return all keys or values.
    */
   CachePublisher<K, V> withAllKeys();

   /**
    * Configures the publisher to only publish keys or values that map to a given segment in the provided
    * IntSet. The {@link org.infinispan.commons.util.IntSets} is recommended to be used. Defaults to "all" segments.
    * <p>
    * Note that if this and {@link #withKeys(Set)} are both used, then a key is only returned if it is also maps to
    * a provided segment.
    * @param segments determines what entries should be evaluated by only using ones that map to the given segments
    * @return a publisher that will filter published values based on the given segments
    * @throws NullPointerException if the provided IntSet is null
    */
   CachePublisher<K, V> withSegments(IntSet segments);

   /**
    * Configures the publisher to publish value(s) irrespective of their mapped segment. Defaults to "all" segments.
    * @return a publisher that will publish all items irrespective of its segment
    */
   CachePublisher<K, V> withAllSegments();

   /**
    * Allows the caller to configure what level of consistency is required when retrieving data. At most is the weakest
    * guarantee which ensures that data in a segment will be read once in a stable topology, but if there is a concurrent
    * topology update a given segment or a portion of its data may not be returned.
    * <p>
    * The default data consistency is {@link #exactlyOnce()}.
    */
   CachePublisher<K, V> atMostOnce();

   /**
    * Allows the caller to configure what level of consistency is required when retrieving data. At least ensures
    * that data in a segment will be read once in a stable topology, but if there is a concurrent
    * topology update a given segment or a portion of its data may be returned multiple times.
    * <p>
    * The default data consistency is {@link #exactlyOnce()}.
    * @return a publisher that will provide at least once data semantics
    */
   CachePublisher<K, V> atLeastOnce();

   /**
    * Allows the caller to configure what level of consistency is required when retrieving data. Exactly once ensures
    * the highest level of guarantee so that even under a topology all data is returned once.
    * <p>
    * Exactly once is the default data consistency level.
    * @return a publisher that will provide exactly once data semantics
    */
   CachePublisher<K, V> exactlyOnce();

   /**
    * Same as {@link #entryReduction(Function, Function)} except that the source publisher provided to the <b>transformer</b>
    * is made up of keys only.
    * @param <R> return value type
    * @return CompletionStage that contains the resulting value when complete
    * @throws NullPointerException if either the transformer or finalizer is null
    */
   <R> CompletionStage<R> keyReduction(Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
                                       Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Same as {@link #keyReduction(Function, Function)} except that the Functions must also implement <code>Serializable</code>.
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>.
    * @param <R> return value type
    * @return CompletionStage that contains the resulting value when complete
    * @throws NullPointerException if either the transformer or finalizer is null
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
    * @param transformer reduces the given publisher of data eventually into a single value.
    * @param finalizer reduces all the single values produced by the transformer or this finalizer into one final value.
    * @return CompletionStage that contains the resulting value when complete
    * @param <R> return value type
    * @throws NullPointerException if either the transformer or finalizer is null
    */
   <R> CompletionStage<R> entryReduction(Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
                                         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Same as {@link #entryReduction(Function, Function)} except that the Functions must also implement <code>Serializable</code>.
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>.
    * @param transformer reduces the given publisher of data eventually into a single value.
    * @param finalizer reduces all the single values produced by the transformer or this finalizer into one final value.
    * @return CompletionStage that contains the resulting value when complete
    * @param <R> return value type
    * @throws NullPointerException if either the transformer or finalizer is null
    */
   <R> CompletionStage<R> entryReduction(SerializableFunction<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
                                         SerializableFunction<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Same as {@link #entryPublisher(Function)} except that the source publisher provided to the <b>transformer</b> is
    * made up of keys only.
    * @param <R> return value type
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    * @throws NullPointerException if either the transformer is null
    */
   <R> SegmentPublisherSupplier<R> keyPublisher(Function<? super Publisher<K>, ? extends Publisher<R>> transformer);

   /**
    * Same as {@link #keyPublisher(Function)} except that the Function must also implement <code>Serializable</code>.
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>.
    * @param <R> return value type
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    * @throws NullPointerException if either the transformer is null
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
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    * @param <R> return value type
    * @throws NullPointerException if either the transformer is null
    */
   <R> SegmentPublisherSupplier<R> entryPublisher(Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer);

   /**
    * Same as {@link #entryPublisher(Function)} except that the Function must also implement <code>Serializable</code>.
    * <p>
    * The compiler will pick this overload for lambda parameters, making them <code>Serializable</code>.
    * @param transformer transform the given stream of data into something else (requires non null)
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    * @param <R> return value type
    * @throws NullPointerException if either the transformer is null
    */
   <R> SegmentPublisherSupplier<R> entryPublisher(SerializableFunction<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer);
}
