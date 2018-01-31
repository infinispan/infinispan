package org.infinispan.stream.impl;

import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;

import io.reactivex.processors.PublishProcessor;

/**
 * Publisher that also allows signaling completed or lost signals. Note this publisher only allows a single subscription
 * at a time.
 * @author wburns
 * @since 9.0
 */
public class TestRemoteIteratorPublisher<T> implements ClusterStreamManager.RemoteIteratorPublisher<T> {
   private PublishProcessor<T> processor;
   private Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments;
   private Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments;

   public TestRemoteIteratorPublisher() {
      processor = PublishProcessor.create();
   }

   private <R> R requireNonNull(R object) {
      return Objects.requireNonNull(object, "No one has subscribed yet!");
   }

   public PublishProcessor<T> publishProcessor() {
      return processor;
   }

   public void notifyCompleted(Supplier<PrimitiveIterator.OfInt> segments) {
      Consumer<? super Supplier<PrimitiveIterator.OfInt>> completed = requireNonNull(completedSegments);
      completed.accept(segments);
   }

   public void notifyLost(Supplier<PrimitiveIterator.OfInt> segments) {
      Consumer<? super Supplier<PrimitiveIterator.OfInt>> lost = requireNonNull(lostSegments);
      lost.accept(segments);
   }

   @Override
   public void subscribe(Subscriber<? super T> s, Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments) {
      if (processor.hasSubscribers()) {
         throw new IllegalStateException("Only 1 subscriber allowed");
      }
      processor.subscribe(s);

      this.completedSegments = Objects.requireNonNull(completedSegments);
      this.lostSegments = Objects.requireNonNull(lostSegments);
   }


}
