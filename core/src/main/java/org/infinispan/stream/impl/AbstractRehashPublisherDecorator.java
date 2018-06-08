package org.infinispan.stream.impl;

import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * Abstract publisher decorator that is used to notify segment listener of loss of segments while entries are
 * being retrieved.
 * @author wburns
 * @since 9.0
 */
public abstract class AbstractRehashPublisherDecorator<S> implements PublisherDecorator<S> {
   final AbstractCacheStream.IteratorOperation iteratorOperation;
   final DistributionManager dm;
   final Address localAddress;
   final Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments;
   final Consumer<Object> keyConsumer;
   final Function<S, ?> toKeyFunction;

   AbstractRehashPublisherDecorator(AbstractCacheStream.IteratorOperation iteratorOperation, DistributionManager dm,
         Address localAddress, Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments,
         Consumer<Object> keyConsumer, Function<S, ?> toKeyFunction) {
      this.iteratorOperation = iteratorOperation;
      this.dm = dm;
      this.localAddress = localAddress;
      this.lostSegments = lostSegments;
      this.keyConsumer = keyConsumer;
      this.toKeyFunction = toKeyFunction;
   }

   abstract Log getLog();

   Publisher<S> decorateLocal(Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
         ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
         Publisher<S> localPublisher) {
      Publisher<S> convertedPublisher = Flowable.fromPublisher(localPublisher).doOnComplete(() -> {
         IntSet ourSegments;
         if (onlyLocal) {
            ourSegments = IntSets.mutableCopyFrom(beginningCh.getSegmentsForOwner(localAddress));
         } else {
            ourSegments = IntSets.mutableCopyFrom(beginningCh.getPrimarySegmentsForOwner(localAddress));
         }
         ourSegments.retainAll(segmentsToFilter);
         // This will notify both completed and suspect of segments that may not even exist or were completed before
         // on a rehash
         if (dm.getReadConsistentHash().equals(beginningCh)) {
            getLog().tracef("Local iterator has completed segments %s", ourSegments);
            completedSegments.accept((Supplier<PrimitiveIterator.OfInt>) ourSegments::iterator);
         } else {
            getLog().tracef("Local iterator segments %s are all suspect as consistent hash has changed", ourSegments);
            lostSegments.accept((Supplier<PrimitiveIterator.OfInt>) ourSegments::iterator);
         }
      });
      return iteratorOperation.handlePublisher(convertedPublisher, keyConsumer, toKeyFunction);
   }
}
