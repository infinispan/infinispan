package org.infinispan.commons.reactive;

import java.util.Map;

import org.infinispan.commons.util.Util;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.processors.AsyncProcessor;
import io.reactivex.rxjava3.processors.FlowableProcessor;

/**
 * Static factory class that provides methods to obtain commonly used instances for interoperation between RxJava
 * and standard JRE.
 * @author wburns
 * @since 10.0
 */
public class RxJavaInterop {
   protected RxJavaInterop() { }

   /**
    * Provides a {@link Function} that can be used to convert from an instance of {@link java.util.Map.Entry} to
    * the key of the entry. This is useful for the instance passed to a method like {@link Flowable#map(Function)}.
    *
    * @param <K> key type
    * @param <V> value type
    * @return rxjava function to convert from a Map.Entry to its key.
    */
   public static <K, V> Function<Map.Entry<K, V>, K> entryToKeyFunction() {
      return (Function) entryToKeyFunction;
   }

   /**
    * Provides a {@link Function} that can be used to convert from an instance of {@link java.util.Map.Entry} to
    * the value of the entry. This is useful for the instance passed to a method like {@link Flowable#map(Function)}.
    *
    * @param <K> key type
    * @param <V> value type
    * @return rxjava function to convert from a Map.Entry to its value.
    */
   public static <K, V> Function<Map.Entry<K, V>, V> entryToValueFunction() {
      return (Function) entryToValueFunction;
   }

   public static <R> Function<? super Throwable, Publisher<R>> cacheExceptionWrapper() {
      return (Function) wrapThrowable;
   }

   public static <R> Function<R, R> identityFunction() {
      return (Function) identityFunction;
   }

   public static <R> Consumer<R> emptyConsumer() {
      return (Consumer) emptyConsumer;
   }

   /**
    * Returns a {@link FlowableProcessor} that is already complete and will ignore any value submitted to it and will
    * immediately cancel any subscriptions it receives.
    * @return processor that is completed
    * @param <R> user value type
    */
   public static <R> FlowableProcessor<R> completedFlowableProcessor() {
      return (FlowableProcessor<R>) completeFlowableProcessor;
   }

   private static final Function<Object, Object> identityFunction = i -> i;
   private static final Consumer<Object> emptyConsumer = ignore -> { };
   private static final Function<Map.Entry<Object, Object>, Object> entryToKeyFunction = Map.Entry::getKey;
   private static final Function<Map.Entry<Object, Object>, Object> entryToValueFunction = Map.Entry::getValue;
   private static final Function<? super Throwable, Publisher<?>> wrapThrowable = t -> Flowable.error(Util.rewrapAsCacheException(t));
   private static final FlowableProcessor<Object> completeFlowableProcessor;

   static {
      completeFlowableProcessor = AsyncProcessor.create();
      completeFlowableProcessor.onComplete();
   }
}
