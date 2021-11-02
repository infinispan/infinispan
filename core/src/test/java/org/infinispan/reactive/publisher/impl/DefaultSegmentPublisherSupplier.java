package org.infinispan.reactive.publisher.impl;

import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

public interface DefaultSegmentPublisherSupplier<R> extends SegmentPublisherSupplier<R> {
   @Override
   default Publisher<Notification<R>> publisherWithSegments() {
      return Flowable.fromPublisher(publisherWithoutSegments())
                     .map(Notifications::value);
   }
}
