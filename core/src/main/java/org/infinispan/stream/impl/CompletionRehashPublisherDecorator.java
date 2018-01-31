package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

import io.reactivex.Flowable;

/**
 * PublisherDecorator that only notifies a user listener of segment completion after the last entry for a given
 * segment has been retrieved from iteration.
 * @author wburns
 * @since 9.0
 */
public class CompletionRehashPublisherDecorator<S> extends RehashPublisherDecorator<S> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> userListener;
   private final List<KeyWatchingCompletionListener> completionListeners;

   CompletionRehashPublisherDecorator(AbstractCacheStream.IteratorOperation iteratorOperation, DistributionManager dm,
         Address localAddress, Consumer<? super Supplier<PrimitiveIterator.OfInt>> userListener,
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments, Consumer<Object> keyConsumer) {
      super(iteratorOperation, dm, localAddress, completedSegments, lostSegments, keyConsumer);
      this.userListener = userListener;
      this.completionListeners = Collections.synchronizedList(new ArrayList<>(4));
   }

   public void valueIterated(Object obj) {
      for (KeyWatchingCompletionListener kwcl : completionListeners) {
         kwcl.valueIterated(obj);
      }
   }

   public void complete() {
      completionListeners.forEach(KeyWatchingCompletionListener::completed);
   }

   @Override
   Log getLog() {
      return log;
   }

   @Override
   public Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher) {
      // We have to have a listener per remote publisher, since we receive results concurrently and we
      // can't properly track the completion of keys per segment without them being separated
      KeyWatchingCompletionListener kwcl = new KeyWatchingCompletionListener(userListener);
      completionListeners.add(kwcl);

      Publisher<S> convertedPublisher = s -> remotePublisher.subscribe(s, i -> {
         // Remote we always notify the provided completed segments as it tracks segment completion
         // for retries
         completedSegments.accept(i);
         // however we have to wait before notifying the user segment completion until
         // we iterate upon the last key of the block of segments
         kwcl.accept(i);
      }, lostSegments);
      // We have to track each key received from this publisher as it would map to all segments when completed
      return Flowable.fromPublisher(iteratorOperation.handlePublisher(convertedPublisher, keyConsumer)).doOnNext(
            kwcl::valueAdded);
   }

   @Override
   public Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
         Publisher<S> localPublisher) {
      KeyWatchingCompletionListener kwcl = new KeyWatchingCompletionListener(userListener);
      completionListeners.add(kwcl);

      Publisher<S> convertedLocalPublisher = decorateLocal(i -> {
         // Remote we always notify the provided completed segments as it tracks segment completion
         // for retries
         completedSegments.accept(i);
         // however we have to wait before notifying the user segment completion until
         // we iterate upon the last key of the block of segments
         kwcl.accept(i);
      }, beginningCh, onlyLocal, segmentsToFilter, localPublisher);
      // We have to track each key received from this publisher as it would map to all segments when completed
      return Flowable.fromPublisher(iteratorOperation.handlePublisher(convertedLocalPublisher, keyConsumer))
            .doOnNext(kwcl::valueAdded);
   }
}
