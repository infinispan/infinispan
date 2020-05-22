package org.infinispan.persistence.support;

import org.infinispan.persistence.spi.NonBlockingStore;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.flowables.GroupedFlowable;

public class SegmentPublisherWrapper<Type> implements NonBlockingStore.SegmentedPublisher<Type> {
   private final GroupedFlowable<Integer, ? extends Type> groupedFlowable;

   private SegmentPublisherWrapper(GroupedFlowable<Integer, ? extends Type> groupedFlowable) {
      this.groupedFlowable = groupedFlowable;
   }

   public static <Type> SegmentPublisherWrapper<Type> wrap(GroupedFlowable<Integer, ? extends Type> groupedFlowable) {
      return new SegmentPublisherWrapper<>(groupedFlowable);
   }

   @Override
   public int getSegment() {
      return groupedFlowable.getKey();
   }

   @Override
   public void subscribe(Subscriber<? super Type> s) {
      groupedFlowable.subscribe(s);
   }
}
