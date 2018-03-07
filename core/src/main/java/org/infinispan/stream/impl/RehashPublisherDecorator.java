package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.PrimitiveIterator;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

/**
 * PublisherDecorator that decorates the publisher to notify of when segments are completed for these invocations
 * @author wburns
 * @since 9.0
 */
class RehashPublisherDecorator<S> extends AbstractRehashPublisherDecorator<S> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   final Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments;
   final Executor executor;
   final Function<S, ?> toKeyFunction;

   RehashPublisherDecorator(AbstractCacheStream.IteratorOperation iteratorOperation, DistributionManager dm,
         Address localAddress, Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments, Executor executor,
         Consumer<Object> keyConsumer, Function<S, ?> toKeyFunction) {
      super(iteratorOperation, dm, localAddress, lostSegments, keyConsumer, toKeyFunction);
      this.completedSegments = completedSegments;
      this.executor = executor;
      this.toKeyFunction = toKeyFunction;
   }

   @Override
   Log getLog() {
      return log;
   }

   Publisher<S> applySubscribeExecutor(Publisher<S> publisher) {
      return Flowable.fromPublisher(publisher).subscribeOn(Schedulers.from(executor));
   }

   @Override
   public Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher) {
      Publisher<S> convertedPublisher = s -> remotePublisher.subscribe(s, completedSegments, lostSegments);
      // When we subscribe on this do it in async thread - including requests so user thread doesn't take
      // the cost of serialization and rpc invocation
      return iteratorOperation.handlePublisher(applySubscribeExecutor(convertedPublisher), keyConsumer, toKeyFunction);
   }

   @Override
   public Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
         Publisher<S> localPublisher) {
      return decorateLocal(completedSegments, beginningCh, onlyLocal, segmentsToFilter, localPublisher);
   }
}
