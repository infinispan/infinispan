package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

/**
 * PublisherDecorator that decorates the publisher to notify of when segments are completed for these invocations
 * @author wburns
 * @since 9.0
 */
class RehashPublisherDecorator<S> extends AbstractRehashPublisherDecorator<S> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   final Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments;

   RehashPublisherDecorator(AbstractCacheStream.IteratorOperation iteratorOperation, DistributionManager dm,
         Address localAddress, Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments, Consumer<Object> keyConsumer) {
      super(iteratorOperation, dm, localAddress, lostSegments, keyConsumer);
      this.completedSegments = completedSegments;
   }

   @Override
   Log getLog() {
      return log;
   }

   @Override
   public Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher) {
      Publisher<S> convertedPublisher = s -> remotePublisher.subscribe(s, completedSegments, lostSegments);
      return iteratorOperation.handlePublisher(convertedPublisher, keyConsumer);
   }

   @Override
   public Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
         Publisher<S> localPublisher) {
      return decorateLocal(completedSegments, beginningCh, onlyLocal, segmentsToFilter, localPublisher);
   }
}
