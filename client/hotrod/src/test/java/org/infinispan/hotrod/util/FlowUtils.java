package org.infinispan.hotrod.util;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import io.reactivex.rxjava3.core.Flowable;
import org.reactivestreams.FlowAdapters;

public final class FlowUtils {

   private FlowUtils() { }

   public static <T> Flowable<T> toFlowable(Flow.Publisher<T> publisher) {
      return Flowable.fromPublisher(FlowAdapters.toPublisher(publisher));
   }

   public static <T, R, A> R blockingCollect(Flowable<T> flowable, Collector<? super T, A, R> collector) {
      return flowable.collect(collector)
            .timeout(10, TimeUnit.SECONDS)
            .blockingGet();
   }

   /**
    * Collect all values in the {@link Flow.Publisher} using the given {@link Collector}.
    * This is a blocking method.
    *
    * @param publisher: Source of the values.
    * @param collector: Collect the values.
    * @return The collected values.
    */
   public static <T, R, A> R blockingCollect(Flow.Publisher<T> publisher, Collector<? super T, A, R> collector) {
      return blockingCollect(toFlowable(publisher), collector);
   }

   /**
    * Collect all values in the {@link Flow.Publisher} into a list.
    * This is a blocking method.
    *
    * @param publisher: Source of the values.
    * @return A list with the elements from {@link Flow.Publisher}.
    * @param <T>: Type of the elements.
    */
   public static <T> List<T> blockingCollect(Flow.Publisher<T> publisher) {
      return blockingCollect(publisher, Collectors.toList());
   }
}
