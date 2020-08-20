package org.infinispan.reactive;

import java.util.concurrent.CompletionStage;

import org.infinispan.util.concurrent.CompletionStages;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.AsyncProcessor;

/**
 * Static factory class that provides methods to obtain commonly used instances for interoperation between RxJava
 * and standard JRE.
 * @author wburns
 * @since 10.0
 */
public class RxJavaInterop extends org.infinispan.commons.reactive.RxJavaInterop {
   private RxJavaInterop() { }

   public static <R> Flowable<R> voidCompletionStageToFlowable(CompletionStage<Void> stage) {
      if (CompletionStages.isCompletedSuccessfully(stage)) {
         return Flowable.empty();
      }
      AsyncProcessor<R> ap = AsyncProcessor.create();

      stage.whenComplete((value, t) -> {
         if (t != null) {
            ap.onError(t);
         } else {
            ap.onComplete();
         }
      });

      return ap;
   }
}
