package org.infinispan.stream.impl;

import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * @author wburns
 * @since 9.0
 */
public class CompletionRehashPublisherDecorator<S> extends RehashPublisherDecorator<S> {
   private final KeyWatchingCompletionListener completionListener;

   CompletionRehashPublisherDecorator(AbstractCacheStream.IteratorOperation iteratorOperation, DistributionManager dm,
         Address localAddress, KeyWatchingCompletionListener completionListener,
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments, Consumer<Object> keyConsumer) {
      super(iteratorOperation, dm, localAddress, completedSegments, lostSegments, keyConsumer);
      this.completionListener = completionListener;
   }

   @Override
   protected Publisher<S> decorateBeforeReturn(Publisher<S> publisher) {
      return Flowable.fromPublisher(super.decorateBeforeReturn(publisher)).doOnNext(
            completionListener::valueAdded);
   }
}
