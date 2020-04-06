package org.infinispan.persistence.support;

import org.infinispan.persistence.spi.NonBlockingStore;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.flowables.GroupedFlowable;

public class SegmentPublisherWrapper<Type> implements NonBlockingStore.SegmentedPublisher<Type> {
   private final GroupedFlowable<Integer, Type> groupedFlowable;

   public SegmentPublisherWrapper(GroupedFlowable<Integer, Type> groupedFlowable) {
      this.groupedFlowable = groupedFlowable;
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
