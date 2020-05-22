package org.infinispan.persistence.support;

import java.util.Objects;

import org.infinispan.persistence.spi.NonBlockingStore;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class SingleSegmentPublisher<E> implements NonBlockingStore.SegmentedPublisher<E> {
   private final int segment;
   private final Publisher<? extends E> publisher;

   public static <E> NonBlockingStore.SegmentedPublisher<E> singleSegment(int segment, Publisher<? extends E> publisher) {
      return new SingleSegmentPublisher<>(segment, publisher);
   }

   public static <E> NonBlockingStore.SegmentedPublisher<E> singleSegment(Publisher<? extends E> publisher) {
      return new SingleSegmentPublisher<>(0, publisher);
   }

   private SingleSegmentPublisher(int segment, Publisher<? extends E> publisher) {
      this.segment = segment;
      this.publisher = Objects.requireNonNull(publisher);
   }

   @Override
   public int getSegment() {
      return segment;
   }

   @Override
   public void subscribe(Subscriber<? super E> s) {
      publisher.subscribe(s);
   }
}
