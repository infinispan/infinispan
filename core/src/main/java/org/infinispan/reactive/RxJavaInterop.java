package org.infinispan.reactive;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.infinispan.commons.util.Util;
import org.reactivestreams.Publisher;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import io.reactivex.internal.functions.Functions;
import io.reactivex.processors.AsyncProcessor;
import io.reactivex.subjects.CompletableSubject;
import io.reactivex.subjects.MaybeSubject;
import io.reactivex.subjects.SingleSubject;

/**
 * Static factory class that provides methods to obtain commonly used instances for interoperation between RxJava
 * and standard JRE.
 * @author wburns
 * @since 10.0
 */
public class RxJavaInterop {
   private RxJavaInterop() { }

   public static Function<Completable, CompletionStage<Void>> completableToCompletionStage() {
      return completableToCompletionStage;
   }
   /**
    * Provides an interop function that can be used to convert a Single into a CompletionStage. Note that this function
    * is not from the standard java.util.function package, but rather {@link Function} to interop better with the
    * {@link Single#to(Function)} method.
    * @param <E> underlying type
    * @return rxjava function to convert Single to CompletionStage
    */
   public static <E> Function<Single<? extends E>, CompletionStage<E>> singleToCompletionStage() {
      return (Function) singleToCompletionStage;
   }

   /**
    * Provides an interop function that can be used to convert a Maybe into a CompletionStage. Note that this function
    * is not from the standard java.util.function package, but rather {@link Function} to interop better with the
    * {@link Maybe#to(Function)} method.
    * @param <E> underlying type
    * @return rxjava function to convert Maybe to CompletionStage
    */
   public static <E> Function<Maybe<E>, CompletionStage<E>> maybeToCompletionStage() {
      return (Function) maybeToCompletionStage;
   }

   /**
    * Provides an interop function that can be used to convert a Flowable into a CompletionStage. Note that this function
    * is not from the standard java.util.function package, but rather {@link Function} to interop better with the
    * {@link Maybe#to(Function)} method. Any published values are ignored and the returned CompletionStage is
    * completed when the Flowable completes or completed exceptionally if the Flowable has an error.
    * @return rxjava function to convert Flowable to CompletionStage
    */
   public static <E> Function<Flowable<E>, CompletionStage<Void>> flowableToCompletionStage() {
      return (Function) flowableToCompletionStage;
   }

   public static Completable completionStageToCompletable(CompletionStage<Void> stage) {
      CompletableSubject cs = CompletableSubject.create();
      stage.whenComplete((o, throwable) -> {
         if (throwable != null) {
            cs.onError(throwable);
         } else {
            cs.onComplete();
         }
      });
      return cs;
   }

   public static java.util.function.Function<CompletionStage<?>, Completable> completionStageToCompletable() {
      return completionStageCompletableFunction;
   }

   public static <E> Single<E> completionStageToSingle(CompletionStage<E> stage) {
      SingleSubject<E> ss = SingleSubject.create();

      stage.whenComplete((value, t) -> {
         if (t != null) {
            ss.onError(t);
         }
         if (value != null) {
            ss.onSuccess(value);
         } else {
            ss.onError(new NoSuchElementException());
         }
      });

      return ss;
   }

   public static <E> Maybe<E> completionStageToMaybe(CompletionStage<E> stage) {
      MaybeSubject<E> ms = MaybeSubject.create();

      stage.whenComplete((value, t) -> {
         if (t != null) {
            ms.onError(t);
         }
         if (value != null) {
            ms.onSuccess(value);
         } else {
            ms.onComplete();
         }
      });

      return ms;
   }

   /**
    * Transforms a {@link Stream} to a {@link Flowable}. Note that the resulting Flowable can only be subscribed to
    * once as a Stream only allows a single terminal operation performed upon it. When the Flowable is completed,
    * either exceptionally or normally, the Stream is also closed
    * @param stream the stream to transform to a Flowable
    * @param <E> inner type
    * @return Flowable that can only be subscribed to once
    */
   public static <E> Flowable<E> fromStream(Stream<E> stream) {
      return Flowable.fromIterable(stream::iterator)
            .doOnTerminate(stream::close);
   }

   /**
    * Provides a {@link Function} that can be used to convert from an instance of {@link java.util.Map.Entry} to
    * the key of the entry. This is useful for the instance passed to a method like {@link Flowable#map(Function)}.
    * @param <K> key type
    * @param <V> value type
    * @return rxjava function to convert from a Map.Entry to its key.
    */
   public static <K, V> Function<Map.Entry<K, V>, K> entryToKeyFunction() {
      return (Function) entryToKeyFunction;
   }

   private static final Function<Completable, CompletionStage<Void>> completableToCompletionStage = completable -> {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      completable.subscribe(() -> cf.complete(null), cf::completeExceptionally);
      return cf;
   };

   public static <R> Function<? super Throwable, Publisher<R>> cacheExceptionWrapper() {
      return (Function) wrapThrowable;
   }

   private static final Function<Single<Object>, CompletionStage<Object>> singleToCompletionStage = single -> {
      CompletableFuture<Object> cf = new CompletableFuture<>();
      single.subscribe(cf::complete, cf::completeExceptionally);
      return cf;
   };

   private static final Function<Flowable<Object>, CompletionStage<Void>> flowableToCompletionStage = flowable -> {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      flowable.subscribe(Functions.emptyConsumer(), cf::completeExceptionally, () -> cf.complete(null));
      return cf;
   };

   private static final Function<Maybe<Object>, CompletionStage<Object>> maybeToCompletionStage = maybe -> {
      CompletableFuture<Object> cf = new CompletableFuture<>();
      maybe.subscribe(cf::complete, cf::completeExceptionally, () -> cf.complete(null));
      return cf;
   };

   private static final java.util.function.Function<CompletionStage<Object>, Flowable<Object>> completionStageToPublisher = stage -> {
      AsyncProcessor<Object> asyncProcessor = AsyncProcessor.create();
      stage.whenComplete((value, t) -> {
         if (t != null) {
            asyncProcessor.onError(t);
         } else {
            if (value != null) {
               asyncProcessor.onNext(value);
            }
            asyncProcessor.onComplete();
         }
      });
      return asyncProcessor;
   };

   private static final Function<Map.Entry<Object, Object>, Object> entryToKeyFunction = Map.Entry::getKey;

   private static final java.util.function.Function<CompletionStage<?>, Completable> completionStageCompletableFunction =
      completionStage -> {
         CompletableSubject cs = CompletableSubject.create();
         completionStage.whenComplete((o, throwable) -> {
            if (throwable != null) {
               cs.onError(throwable);
            } else {
               cs.onComplete();
            }
         });
         return cs;
      };

   private static final Function<? super Throwable, Publisher<?>> wrapThrowable = t -> Flowable.error(Util.rewrapAsCacheException(t));
}
