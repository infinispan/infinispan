package org.infinispan.api.collections.reactive;

import java.util.Comparator;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

/**
 * I -> Infinispan xD
 *
 * @param <T>
 * @author wburns
 */
public interface CachePublisher<T> {

   // Intermediate methods
   <R> CachePublisher<R> map(Function<? super T, ? extends R> function);

   <R> CachePublisher<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> function);

   CachePublisher<T> filter(Predicate<? super T> predicate);

   CachePublisher<T> distributedBatchSize(int batchSize);

   CachePublisher<T> parallel();

   CachePublisher<T> sequential();

   // Terminal Operations

   CachePublisher<T> min(Comparator<? super T> comparator, int amountToReturn);

   CompletionStage<Long> count();

   CompletionStage<Boolean> allMatch(Predicate<? super T> predicate);

   CompletionStage<Boolean> anyMatch(Predicate<? super T> predicate);

   CompletionStage<Boolean> noneMatch(Predicate<? super T> predicate);

   void subscribe(Consumer<? super T> consumer);

   Publisher<T> asPublisher() ;
}
