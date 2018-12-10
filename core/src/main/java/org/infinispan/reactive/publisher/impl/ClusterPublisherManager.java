package org.infinispan.reactive.publisher.impl;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
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
    * Same as {@link #entryReduction(boolean, IntSet, Set, Set, boolean, DeliveryGuarantee, Function, Function)}
    * except that the source publisher provided to the <b>transformer</b> is made up of keys only.
    * @param <R> return value type
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<R> keyReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Performs the given <b>transformer</b> and <b>finalizer</b> on data in the cache, resulting in a single value.
    * Depending on the <b>deliveryGuarantee</b> the <b>transformer</b> may be invoked <b>1..numSegments</b> times. It
    * could be that the <b>transformer</b> is invoked for every segment and produces a result. All of these results
    * are then fed into the <b>finalizer</b> to produce a final result. If publisher is parallel the <b>finalizer</b>
    * will be invoked on each node to ensure there is only a single result per node.
    * @param <R> return value type
    * @param parallelPublisher Whether on each node the publisher should be parallelized remotely and locally
    * @param segments determines what entries should be evaluated by only using ones that map to the given segments
    * @param keysToInclude set of keys that should only be used. If null all entries for the given segments will be evaluated
    * @param keysToExclude set of keys that should not be used. May be null, in which case all provided entries will be evaluated
    * @param includeLoader whether to include entries from the underlying cache loader if any
    * @param deliveryGuarantee delivery guarantee for given entries
    * @param transformer reduces the given publisher of data eventually into a single value. Must not be null.
    * @param finalizer reduces all of the single values produced by the transformer or this finalizer into one final value. Must not be null.
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<R> entryReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);
}
