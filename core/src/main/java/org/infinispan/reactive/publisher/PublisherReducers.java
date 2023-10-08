package org.infinispan.reactive.publisher;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;

/**
 * Static factory method class to provide various reducers and finalizers for use with distributed Publisher. Note
 * that these functions are all serializable by Infinispan assuming that any passed arguments are as well.
 * @author wburns
 * @since 10.0
 */
public class PublisherReducers {
   private PublisherReducers() { }

   public static Function<Publisher<Boolean>, CompletionStage<Boolean>> and() {
      return AndFinalizer.INSTANCE;
   }

   public static <E> Function<Publisher<E>, CompletionStage<Boolean>> allMatch(Predicate<? super E> predicate) {
      return new AllMatchReducer<>(MarshallableObject.create(predicate));
   }

   public static <E> Function<Publisher<E>, CompletionStage<Boolean>> anyMatch(Predicate<? super E> predicate) {
      return new AnyMatchReducer<>(MarshallableObject.create(predicate));
   }

   public static <I, E> Function<Publisher<I>, CompletionStage<E>> collect(Supplier<E> supplier, BiConsumer<E, ? super I> consumer) {
      return new CollectReducer<>(MarshallableObject.create(supplier), MarshallableObject.create(consumer));
   }

   public static <I, E> Function<Publisher<I>, CompletionStage<E>> collectorReducer(Collector<? super I, E, ?> collector) {
      return new CollectorReducer<>(MarshallableObject.create(collector));
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> collectorFinalizer(Collector<?, E, ?> collector) {
      return new CollectorFinalizer<>(MarshallableObject.create(collector));
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> accumulate(BiConsumer<E, E> biConsumer) {
      return new CombinerFinalizer<>(MarshallableObject.create(biConsumer));
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> findFirst() {
      return FindFirstReducerFinalizer.INSTANCE;
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> max(Comparator<? super E> comparator) {
      return new MaxReducerFinalizer<>(MarshallableObject.create(comparator));
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> min(Comparator<? super E> comparator) {
      return new MinReducerFinalizer<>(MarshallableObject.create(comparator));
   }

   public static <E> Function<Publisher<E>, CompletionStage<Boolean>> noneMatch(Predicate<? super E> predicate) {
      return new NoneMatchReducer<>(MarshallableObject.create(predicate));
   }

   public static Function<Publisher<Boolean>, CompletionStage<Boolean>> or() {
      return OrFinalizer.INSTANCE;
   }

   /**
    * Provides a reduction where the initial value must be the identity value that is not modified via the provided
    * biFunction. Failure to do so will cause unexpected results.
    * <p>
    * If the initial value needs to be modified, you should use {@link #reduceWith(Callable, BiFunction)} instead.
    * @param identity initial identity value to use (this value must not be modified by the provide biFunction)
    * @param biFunction biFunction used to reduce the values into a single one
    * @param <I> input type
    * @param <E> output reduced type
    * @return function that will map a publisher of the input type to a completion stage of the output type
    */
   public static <I, E> Function<Publisher<I>, CompletionStage<E>> reduce(E identity,
         BiFunction<E, ? super I, E> biFunction) {
      return new ReduceWithIdentityReducer<>(MarshallableObject.create(identity), MarshallableObject.create(biFunction));
   }

   public static <I, E> Function<Publisher<I>, CompletionStage<E>> reduceWith(Callable<? extends E> initialSupplier,
         BiFunction<E, ? super I, E> biFunction) {
      return new ReduceWithInitialSupplierReducer<>(MarshallableObject.create(initialSupplier), MarshallableObject.create(biFunction));
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> reduce(BinaryOperator<E> operator) {
      return new ReduceReducerFinalizer<>(MarshallableObject.create(operator));
   }

   public static Function<Publisher<?>, CompletionStage<Long>> count() {
      return SumReducer.INSTANCE;
   }

   public static Function<Publisher<Long>, CompletionStage<Long>> add() {
      return SumFinalizer.INSTANCE;
   }

   public static <I> Function<Publisher<I>, CompletionStage<Object[]>> toArrayReducer() {
      return toArrayReducer(null);
   }

   public static <I extends E, E> Function<Publisher<I>, CompletionStage<E[]>> toArrayReducer(IntFunction<E[]> generator) {
      return new ToArrayReducer<>(MarshallableObject.create(generator));
   }

   public static <E> Function<Publisher<E[]>, CompletionStage<E[]>> toArrayFinalizer() {
      return toArrayFinalizer(null);
   }

   public static <E> Function<Publisher<E[]>, CompletionStage<E[]>> toArrayFinalizer(IntFunction<E[]> generator) {
      return new ToArrayFinalizer<>(MarshallableObject.create(generator));
   }

   @ProtoTypeId(ProtoStreamTypeIds.ALL_MATCH_REDUCER)
   public static class AllMatchReducer<E> implements Function<Publisher<E>, CompletionStage<Boolean>> {

      @ProtoField(1)
      final MarshallableObject<Predicate<? super E>> predicate;

      @ProtoFactory
      AllMatchReducer(MarshallableObject<Predicate<? super E>> predicate) {
         this.predicate = predicate;
      }

      @Override
      public CompletionStage<Boolean> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .all(predicate.get()::test)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.ANY_MATCH_REDUCER)
   public static class AnyMatchReducer<E> implements Function<Publisher<E>, CompletionStage<Boolean>> {

      @ProtoField(1)
      final MarshallableObject<Predicate<? super E>> predicate;

      @ProtoFactory
      AnyMatchReducer(MarshallableObject<Predicate<? super E>> predicate) {
         this.predicate = predicate;
      }

      @Override
      public CompletionStage<Boolean> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .any(predicate.get()::test)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.AND_FINALIZER)
   public static final class AndFinalizer implements Function<Publisher<Boolean>, CompletionStage<Boolean>> {
      private static final AndFinalizer INSTANCE = new AndFinalizer();

      @ProtoFactory
      static AndFinalizer protoFactory() {
         return INSTANCE;
      }

      @Override
      public CompletionStage<Boolean> apply(Publisher<Boolean> booleanPublisher) {
         return Flowable.fromPublisher(booleanPublisher)
               .all(bool -> bool == Boolean.TRUE)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.COLLECTOR_FINALIZER)
   public static final class CollectorFinalizer<E, R> implements Function<Publisher<E>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<Collector<?, E, ?>> collector;

      @ProtoFactory
      CollectorFinalizer(MarshallableObject<Collector<?, E, ?>> collector) {
         this.collector = collector;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         Collector<?, E, ?> collector = this.collector.get();
         return Flowable.fromPublisher(ePublisher)
               .reduce(collector.combiner()::apply)
               // This is to ensure at least the default value is provided - this shouldnt be required - but
               // the disconnect between reducer and finalizer for collector leaves this ambiguous
               .switchIfEmpty(Single.fromCallable(collector.supplier()::get))
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.COLLECT_REDUCER)
   public static final class CollectReducer<I, E> implements Function<Publisher<I>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<Supplier<E>> supplier;

      @ProtoField(2)
      final MarshallableObject<BiConsumer<E, ? super I>> accumulator;

      @ProtoFactory
      CollectReducer(MarshallableObject<Supplier<E>> supplier, MarshallableObject<BiConsumer<E, ? super I>> accumulator) {
         this.supplier = supplier;
         this.accumulator = accumulator;
      }

      @Override
      public CompletionStage<E> apply(Publisher<I> iPublisher) {
         return Flowable.fromPublisher(iPublisher)
               .collect(supplier.get()::get, accumulator.get()::accept)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.COLLECTOR_REDUCER)
   public static final class CollectorReducer<I, E> implements Function<Publisher<I>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<Collector<? super I, E, ?>> collector;

      @ProtoFactory
      CollectorReducer(MarshallableObject<Collector<? super I, E, ?>> collector) {
         this.collector = collector;
      }

      @Override
      public CompletionStage<E> apply(Publisher<I> iPublisher) {
         Collector<? super I, E, ?> collector = this.collector.get();
         return Flowable.fromPublisher(iPublisher)
               .collect(collector.supplier()::get, collector.accumulator()::accept)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.COMBINER_FINALIZER)
   public static final class CombinerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<BiConsumer<E, E>> biConsumer;

      @ProtoFactory
      CombinerFinalizer(MarshallableObject<BiConsumer<E, E>> biConsumer) {
         this.biConsumer = biConsumer;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce((e1, e2) -> {
                  biConsumer.get().accept(e1, e2);
                  return e1;
               })
               .toCompletionStage(null);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.FIND_FIRST_REDUCER_FINALIZER)
   public static final class FindFirstReducerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {
      private static final FindFirstReducerFinalizer INSTANCE = new FindFirstReducerFinalizer();

      @ProtoFactory
      static FindFirstReducerFinalizer protoFactory() {
         return INSTANCE;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .firstElement()
               .toCompletionStage(null);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MAX_REDUCER_FINALIZER)
   public static class MaxReducerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<Comparator<? super E>> comparator;

      @ProtoFactory
      MaxReducerFinalizer(MarshallableObject<Comparator<? super E>> comparator) {
         this.comparator = comparator;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce((e1, e2) -> {
                  if (comparator.get().compare(e1, e2) > 0) {
                     return e1;
                  }
                  return e2;
               })
               .toCompletionStage(null);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.MIN_REDUCER_FINALIZER)
   public static class MinReducerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<Comparator<? super E>> comparator;

      @ProtoFactory
      MinReducerFinalizer(MarshallableObject<Comparator<? super E>> comparator) {
         this.comparator = comparator;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce((e1, e2) -> {
                  if (comparator.get().compare(e1, e2) > 0) {
                     return e2;
                  }
                  return e1;
               })
               .toCompletionStage(null);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.NONE_MATCH_REDUCER)
   public static class NoneMatchReducer<E> implements Function<Publisher<E>, CompletionStage<Boolean>> {

      @ProtoField(1)
      final MarshallableObject<Predicate<? super E>> predicate;

      @ProtoFactory
      NoneMatchReducer(MarshallableObject<Predicate<? super E>> predicate) {
         this.predicate = predicate;
      }

      @Override
      public CompletionStage<Boolean> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .all(predicate.get().negate()::test)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.OR_FINALIZER)
   public static final class OrFinalizer implements Function<Publisher<Boolean>, CompletionStage<Boolean>> {
      private static final OrFinalizer INSTANCE = new OrFinalizer();

      @ProtoFactory
      static OrFinalizer protoFactory() {
         return INSTANCE;
      }

      @Override
      public CompletionStage<Boolean> apply(Publisher<Boolean> booleanPublisher) {
         return Flowable.fromPublisher(booleanPublisher)
               .any(bool -> bool == Boolean.TRUE)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.REDUCE_WITH_IDENTITY_REDUCER)
   public static class ReduceWithIdentityReducer<I, E> implements Function<Publisher<I>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<E> identity;

      @ProtoField(2)
      final MarshallableObject<BiFunction<E, ? super I, E>> biFunction;

      @ProtoFactory
      ReduceWithIdentityReducer(MarshallableObject<E> identity, MarshallableObject<BiFunction<E, ? super I, E>> biFunction) {
         this.identity = identity;
         this.biFunction = biFunction;
      }

      @Override
      public CompletionStage<E> apply(Publisher<I> iPublisher) {
         return Flowable.fromPublisher(iPublisher)
               .reduce(identity.get(), biFunction.get()::apply)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.REDUCE_WITH_INITIAL_SUPPLIER_REDUCER)
   public static class ReduceWithInitialSupplierReducer<I, E> implements Function<Publisher<I>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<Callable<? extends E>> initialSupplier;

      @ProtoField(2)
      final MarshallableObject<BiFunction<E, ? super I, E>> biFunction;

      @ProtoFactory
      ReduceWithInitialSupplierReducer(MarshallableObject<Callable<? extends E>> initialSupplier,
                                       MarshallableObject<BiFunction<E, ? super I, E>> biFunction) {
         this.initialSupplier = initialSupplier;
         this.biFunction = biFunction;
      }

      @Override
      public CompletionStage<E> apply(Publisher<I> iPublisher) {
         return Flowable.fromPublisher(iPublisher)
               .reduceWith(initialSupplier.get()::call, biFunction.get()::apply)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.REDUCE_REDUCER_FINALIZER)
   public static class ReduceReducerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {

      @ProtoField(1)
      final MarshallableObject<BinaryOperator<E>> operator;

      @ProtoFactory
      ReduceReducerFinalizer(MarshallableObject<BinaryOperator<E>> operator) {
         this.operator = operator;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce(operator.get()::apply)
               .toCompletionStage(null);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.SUM_REDUCER)
   public static class SumReducer implements Function<Publisher<?>, CompletionStage<Long>> {
      private static final SumReducer INSTANCE = new SumReducer();

      @ProtoFactory
      static SumReducer protoFactory() {
         return INSTANCE;
      }

      @Override
      public CompletionStage<Long> apply(Publisher<?> longPublisher) {
         return Flowable.fromPublisher(longPublisher)
               .count()
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.SUM_FINALIZER)
   public static class SumFinalizer implements Function<Publisher<Long>, CompletionStage<Long>> {
      private static final SumFinalizer INSTANCE = new SumFinalizer();

      @ProtoFactory
      static SumFinalizer protoFactory() {
         return INSTANCE;
      }

      @Override
      public CompletionStage<Long> apply(Publisher<Long> longPublisher) {
         return Flowable.fromPublisher(longPublisher)
               .reduce((long) 0, Long::sum)
               .toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.TO_ARRAY_REDUCER)
   public static class ToArrayReducer<I extends E, E> implements Function<Publisher<I>, CompletionStage<E[]>> {

      @ProtoField(1)
      final MarshallableObject<IntFunction<E[]>> generator;

      @ProtoFactory
      ToArrayReducer(MarshallableObject<IntFunction<E[]>> generator) {
         this.generator = generator;
      }

      @Override
      public CompletionStage<E[]> apply(Publisher<I> ePublisher) {
         Single<List<I>> listSingle = Flowable.fromPublisher(ePublisher).toList();
         Single<E[]> arraySingle;
         if (generator != null) {
            arraySingle = listSingle.map(l -> {
               E[] array = generator.get().apply(l.size());
               int offset = 0;
               for (E e : l) {
                  array[offset++] = e;
               }
               return array;
            });
         } else {
            arraySingle = listSingle.map(l -> l.toArray((E[]) Util.EMPTY_OBJECT_ARRAY));
         }
         return arraySingle.toCompletionStage();
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.TO_ARRAY_FINALIZER)
   public static class ToArrayFinalizer<E> implements Function<Publisher<E[]>, CompletionStage<E[]>> {

      @ProtoField(1)
      final MarshallableObject<IntFunction<E[]>> generator;

      @ProtoFactory
      ToArrayFinalizer(MarshallableObject<IntFunction<E[]>> generator) {
         this.generator = generator;
      }

      @Override
      public CompletionStage<E[]> apply(Publisher<E[]> publisher) {
         Flowable<E[]> flowable = Flowable.fromPublisher(publisher);
         Single<E[]> arraySingle;
         if (generator != null) {
            IntFunction<E[]> generator = this.generator.get();
            arraySingle = flowable.reduce((v1, v2) -> {
               E[] array = generator.apply(v1.length + v2.length);
               System.arraycopy(v1, 0, array, 0, v1.length);
               System.arraycopy(v2, 0, array, v1.length, v2.length);
               return array;
            }).switchIfEmpty(Single.fromCallable(() -> generator.apply(0)));
         } else {
            arraySingle = flowable.reduce((v1, v2) -> {
               E[] array = Arrays.copyOf(v1, v1.length + v2.length);
               System.arraycopy(v2, 0, array, v1.length, v2.length);
               return array;
            }).switchIfEmpty(Single.just((E[]) Util.EMPTY_OBJECT_ARRAY));
         }
         return arraySingle.toCompletionStage();
      }
   }
}
