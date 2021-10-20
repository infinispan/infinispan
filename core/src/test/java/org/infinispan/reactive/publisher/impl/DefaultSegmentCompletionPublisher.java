package org.infinispan.reactive.publisher.impl;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public interface DefaultSegmentCompletionPublisher<R> extends SegmentCompletionPublisher<R> {

   @Override
   default void subscribeWithSegments(Subscriber<? super Notification<R>> subscriber) {
      subscribe(new Subscriber<R>() {
         @Override
         public void onSubscribe(Subscription s) {
            subscriber.onSubscribe(s);
         }

         @Override
         public void onNext(R r) {
            subscriber.onNext(Notifications.value(r));
         }

         @Override
         public void onError(Throwable t) {
            subscriber.onError(t);
         }

         @Override
         public void onComplete() {
            subscriber.onComplete();
         }
      });
   }
}
