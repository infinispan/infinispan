package org.infinispan.reactive.publisher.impl;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.reactivestreams.Publisher;

/**
 * Handles locally publishing entries from the cache. This manager will return results that contains suspected segments
 * @param <K> The key type for the underlying cache
 * @param <V> the value type for the underlying cache
 * @author wburns
 * @since 10.0
 */
public interface LocalPublisherManager<K, V> {
   /**
    * Same as {@link #entryReduction(boolean, IntSet, Set, Set, boolean, DeliveryGuarantee, Function, Function)}
    * except that the source publisher provided to the <b>transformer</b> is made up of keys only.
    * @param <R> return value type
    * @param parallelPublisher
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<PublisherResult<R>> keyReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Performs the given <b>transformer</b> and <b>finalizer</b> on data in the cache that is local, resulting in a
    * single value. Depending on the <b>deliveryGuarantee</b> the <b>transformer</b> may be invoked <b>1..numSegments</b>
    * times. It could be that the <b>transformer</b> is invoked for every segment and produces a result. All of these
    * results are then fed into the <b>finalizer</b> to produce a final result. If publisher is parallel the <b>finalizer</b>
    * will be invoked on each node to ensure there is only a single result per node.
    * <p>
    * The effects of the provided <b>deliveryGuarantee</b> are as follows:
    * <table>
    *    <tr>
    *       <th>Guarantee</th><th>Parallel</th><th>Behavior></th>
    *    </tr>
    *    <tr>
    *       <td>AT_MOST_ONCE</td> <td>TRUE</td><td>Each segment is a publisher passed to the transformer individually. Each result of the transformer is supplied to the finalizer. All segments are always complete, ignoring loss of data</td>
    *    </tr>
    *    <tr>
    *       <td>AT_MOST_ONCE</td> <td>FALSE</td><td>A single publisher for all segments is created and passed to the transformer. That result is returned, finalizer is never used All segments are always complete, ignoring loss of data</td>
    *    </tr>
    *    <tr>
    *       <td>AT_LEAST_ONCE</td> <td>TRUE</td><td>Same as AT_MOST_ONCE, but if a segment is lost in the middle it is returned as a suspected segment always returning all values</td>
    *    </tr>
    *    <tr>
    *       <td>AT_LEAST_ONCE</td> <td>FALSE</td><td>Same as AT_MOST_ONCE, but if a segment is lost in the middle it is returned as a suspected segment always returning all values</td>
    *    </tr>
    *    <tr>
    *       <td>EXACTLY_ONCE</td> <td>TRUE</td><td>Each segment is a publisher passed to the transformer individually. Each result is only accepted if the segment was owned the entire duration of the Subscription.</td>
    *    </tr>
    *    <tr>
    *       <td>AT_LEAST_ONCE</td> <td>FALSE</td><td>Same as EXACTLY_ONCE/TRUE, except the publishers are consumed one at a time.</td>
    *    </tr>
    * </table>
    *
    * @param <R> return value type
    * @param parallelPublisher Whether the publisher should be parallelized
    * @param segments determines what entries should be evaluated by only using ones that map to the given segments
    * @param keysToInclude set of keys that should only be used. If null all entries for the given segments will be evaluated
    * @param keysToExclude set of keys that should not be used. May be null, in which case all provided entries will be evaluated
    * @param includeLoader whether to include entries from the underlying cache loader if any
    * @param deliveryGuarantee delivery guarantee for given entries
    * @param transformer reduces the given publisher of data eventually into a single value. Must not be null.
    * @param finalizer reduces all of the single values produced by the transformer or this finalizer into one final value. May be null if not parallel
    * @return CompletionStage that contains the resulting value when complete
    */
   <R> CompletionStage<PublisherResult<R>> entryReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

   /**
    * Method to invoke when a set of segments are being removed from this node. This way operations can be aware
    * of possible data loss while processing.
    * @param lostSegments the segments that are being removed from this node
    */
   void segmentsLost(IntSet lostSegments);
}
