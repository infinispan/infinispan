package org.infinispan.reactive;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.AsyncProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

/**
 * Static factory class that provides methods to obtain commonly used instances for interoperation between RxJava
 * and standard JRE.
 * @author wburns
 * @since 10.0
 */
public class RxJavaInterop extends org.infinispan.commons.reactive.RxJavaInterop {
   private RxJavaInterop() { }

   protected final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

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

   /**
    * Same as {@link #voidCompletionStageToFlowable(CompletionStage)} except that you can optionally have it so that if
    * a throwable occurs in the stage that it isn't propagated if the returned Flowable's subscription was cancelled.
    * <p>
    * This method also only allows for a single subscriber to the Flowable, any additional subscribers will receive
    * an exception when subscribing to the returned Flowable.
    * {@link #voidCompletionStageToFlowable(CompletionStage)} can support any number of subscribers.
    * @param stage stage to complete
    * @param ignoreErrorIfCancelled whether to ignore an error if cancelled
    * @param <R> stage type
    * @return a Flowable that is completed when the stage is
    */
   public static <R> Flowable<R> voidCompletionStageToFlowable(CompletionStage<Void> stage, boolean ignoreErrorIfCancelled) {
      if (!ignoreErrorIfCancelled) {
         return voidCompletionStageToFlowable(stage);
      }
      if (CompletionStages.isCompletedSuccessfully(stage)) {
         return Flowable.empty();
      }
      AtomicBoolean cancelled = new AtomicBoolean();
      UnicastProcessor<R> ap = UnicastProcessor.create(1, () -> cancelled.set(true));

      stage.whenComplete((value, t) -> {
         if (t != null) {
            if (!cancelled.get()) {
               ap.onError(t);
            } else {
               log.debug("Ignoring throwable as the UnicastProcessor is already completed", t);
            }
         } else {
            ap.onComplete();
         }
      });

      return ap;
   }
}
