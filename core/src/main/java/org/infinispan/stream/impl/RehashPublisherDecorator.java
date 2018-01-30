package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * @author wburns
 * @since 9.0
 */
class RehashPublisherDecorator<S> implements PublisherDecorator<S> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   final AbstractCacheStream.IteratorOperation iteratorOperation;
   final DistributionManager dm;
   final Address localAddress;
   final Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments;
   final Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments;
   final Consumer<Object> keyConsumer;

   RehashPublisherDecorator(AbstractCacheStream.IteratorOperation iteratorOperation, DistributionManager dm,
         Address localAddress, Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments, Consumer<Object> keyConsumer) {
      this.iteratorOperation = iteratorOperation;
      this.dm = dm;
      this.localAddress = localAddress;
      this.completedSegments = completedSegments;
      this.lostSegments = lostSegments;
      this.keyConsumer = keyConsumer;
   }

   @Override
   public Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher) {
      Publisher<S> convertedPublisher = s -> remotePublisher.subscribe(s, completedSegments, lostSegments);
      return decorateBeforeReturn(convertedPublisher);
   }

   @Override
   public Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
         Publisher<S> localPublisher) {
      Publisher<S> convertedPublisher = Flowable.fromPublisher(localPublisher).doOnComplete(() -> {
         IntSet ourSegments;
         if (onlyLocal) {
            ourSegments = SmallIntSet.from(beginningCh.getSegmentsForOwner(localAddress));
         } else {
            ourSegments = SmallIntSet.from(beginningCh.getPrimarySegmentsForOwner(localAddress));
         }
         ourSegments.retainAll(segmentsToFilter);
         // This will notify both completed and suspect of segments that may not even exist or were completed before
         // on a rehash
         if (dm.getReadConsistentHash().equals(beginningCh)) {
            log.tracef("Local iterator has completed segments %s", ourSegments);
            completedSegments.accept((Supplier<PrimitiveIterator.OfInt>) ourSegments::iterator);
         } else {
            log.tracef("Local iterator segments %s are all suspect as consistent hash has changed", ourSegments);
            lostSegments.accept((Supplier<PrimitiveIterator.OfInt>) ourSegments::iterator);
         }
      });
      return decorateBeforeReturn(convertedPublisher);
   }

   protected Publisher<S> decorateBeforeReturn(Publisher<S> publisher) {
      return iteratorOperation.handlePublisher(publisher, keyConsumer);
   }
}
