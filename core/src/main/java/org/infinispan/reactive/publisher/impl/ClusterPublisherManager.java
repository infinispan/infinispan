package org.infinispan.reactive.publisher.impl;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.reactivestreams.Publisher;

/**
 * Manages distribution of various publisher operations that are ran locally and/or sent to remote nodes.
 * @param <K> The key type for the underlying cache
 * @param <V> the value type for the underlying cache
 * @author wburns
 * @since 10.0
 */
public interface ClusterPublisherManager<K, V> {
   /**
    * Same as {@link #entryReduction(boolean, IntSet, Set, InvocationContext, long, DeliveryGuarantee, Function, Function)}
    * except that the source publisher provided to the <b>transformer</b> is made up of keys only.
    * @param <R> return value type
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<R> keyReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, InvocationContext invocationContext, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

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
    * @param parallelPublisher Whether on each node the publisher should be parallelized remotely and locally
    * @param segments determines what entries should be evaluated by only using ones that map to the given segments (if null assumes all segments)
    * @param keysToInclude set of keys that should only be used (if null all entries for the given segments will be evaluated)
    * @param invocationContext context of the invoking operation, context entries override the values in the cache (may be null)
    * @param explicitFlags cache flags, which are passed to {@link org.infinispan.commands.read.KeySetCommand} or {@link org.infinispan.commands.read.EntrySetCommand}
    * @param deliveryGuarantee delivery guarantee for given entries
    * @param transformer reduces the given publisher of data eventually into a single value. Must not be null.
    * @param finalizer reduces all of the single values produced by the transformer or this finalizer into one final value. Must not be null.
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<R> entryReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, InvocationContext invocationContext, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Same as {@link #entryPublisher(IntSet, Set, InvocationContext, long, DeliveryGuarantee, int, Function)}
    * except that the source publisher provided to the <b>transformer</b> is made up of keys only.
    * @param <R> return value type
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    */
   <R> SegmentPublisherSupplier<R> keyPublisher(IntSet segments, Set<K> keysToInclude, InvocationContext invocationContext,
         long explicitFlags, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<K>, ? extends Publisher<R>> transformer);

   /**
    * Performs the given <b>transformer</b> on data in the cache, resulting in multiple values. If a single
    * value is desired, the user should use {@link #entryReduction(boolean, IntSet, Set, InvocationContext, long, DeliveryGuarantee, Function, Function)}
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
    * @param segments determines what entries should be evaluated by only using ones that map to the given segments (if null assumes all segments)
    * @param keysToInclude set of keys that should only be used (if null all entries for the given segments will be evaluated)
    * @param invocationContext context of the invoking operation, context entries override the values in the cache (may be null)
    * @param explicitFlags cache flags
    * @param deliveryGuarantee delivery guarantee for given entries
    * @param batchSize how many entries to be returned at a given time
    * @param transformer transform the given stream of data into something else (requires non null)
    * @param <R> return value type
    * @return Publisher that when subscribed to will return the results and notify of segment completion if necessary
    */
   <R> SegmentPublisherSupplier<R> entryPublisher(IntSet segments, Set<K> keysToInclude, InvocationContext invocationContext,
         long explicitFlags, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer);

   CompletionStage<Long> sizePublisher(IntSet segments, InvocationContext ctx, long flags);
}
