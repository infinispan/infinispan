package org.infinispan.reactive.publisher;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
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
      return new AllMatchReducer<>(predicate);
   }


   public static <E> Function<Publisher<E>, CompletionStage<Boolean>> anyMatch(Predicate<? super E> predicate) {
      return new AnyMatchReducer<>(predicate);
   }

   public static <I, E> Function<Publisher<I>, CompletionStage<E>> collect(Supplier<E> supplier, BiConsumer<E, ? super I> consumer) {
      return new CollectReducer<>(supplier, consumer);
   }

   public static <I, E> Function<Publisher<I>, CompletionStage<E>> collectorReducer(Collector<? super I, E, ?> collector) {
      return new CollectorReducer<>(collector);
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> collectorFinalizer(Collector<?, E, ?> collector) {
      return new CollectorFinalizer<>(collector);
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> accumulate(BiConsumer<E, E> biConsumer) {
      return new CombinerFinalizer<>(biConsumer);
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> findFirst() {
      return FindFirstReducerFinalizer.INSTANCE;
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> max(Comparator<? super E> comparator) {
      return new MaxReducerFinalizer<>(comparator);
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> min(Comparator<? super E> comparator) {
      return new MinReducerFinalizer<>(comparator);
   }

   public static <E> Function<Publisher<E>, CompletionStage<Boolean>> noneMatch(Predicate<? super E> predicate) {
      return new NoneMatchReducer<>(predicate);
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
      return new ReduceWithIdentityReducer<>(identity, biFunction);
   }

   public static <I, E> Function<Publisher<I>, CompletionStage<E>> reduceWith(Callable<? extends E> initialSupplier,
         BiFunction<E, ? super I, E> biFunction) {
      return new ReduceWithInitialSupplierReducer<>(initialSupplier, biFunction);
   }

   public static <E> Function<Publisher<E>, CompletionStage<E>> reduce(BinaryOperator<E> operator) {
      return new ReduceReducerFinalizer<>(operator);
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
      return new ToArrayReducer<>(generator);
   }

   public static <E> Function<Publisher<E[]>, CompletionStage<E[]>> toArrayFinalizer() {
      return toArrayFinalizer(null);
   }

   public static <E> Function<Publisher<E[]>, CompletionStage<E[]>> toArrayFinalizer(IntFunction<E[]> generator) {
      return new ToArrayFinalizer<>(generator);
   }

   private static class AllMatchReducer<E> implements Function<Publisher<E>, CompletionStage<Boolean>> {
      private final Predicate<? super E> predicate;

      private AllMatchReducer(Predicate<? super E> predicate) {

         this.predicate = predicate;
      }

      @Override
      public CompletionStage<Boolean> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .all(predicate::test)
               .toCompletionStage();
      }
   }

   private static class AnyMatchReducer<E> implements Function<Publisher<E>, CompletionStage<Boolean>> {
      private final Predicate<? super E> predicate;

      private AnyMatchReducer(Predicate<? super E> predicate) {

         this.predicate = predicate;
      }

      @Override
      public CompletionStage<Boolean> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .any(predicate::test)
               .toCompletionStage();
      }
   }

   private static final class AndFinalizer implements Function<Publisher<Boolean>, CompletionStage<Boolean>> {
      private static final AndFinalizer INSTANCE = new AndFinalizer();

      @Override
      public CompletionStage<Boolean> apply(Publisher<Boolean> booleanPublisher) {
         return Flowable.fromPublisher(booleanPublisher)
               .all(bool -> bool == Boolean.TRUE)
               .toCompletionStage();
      }
   }

   private static final class CollectorFinalizer<E, R> implements Function<Publisher<E>, CompletionStage<E>> {
      private final Collector<?, E, ?> collector;

      private CollectorFinalizer(Collector<?, E, ?> collector) {
         this.collector = collector;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce(collector.combiner()::apply)
               // This is to ensure at least the default value is provided - this shouldnt be required - but
               // the disconnect between reducer and finalizer for collector leaves this ambiguous
               .switchIfEmpty(Single.fromCallable(collector.supplier()::get))
               .toCompletionStage();
      }
   }

   private static final class CollectReducer<I, E> implements Function<Publisher<I>, CompletionStage<E>> {
      private final Supplier<E> supplier;
      private final BiConsumer<E, ? super I> accumulator;

      private CollectReducer(Supplier<E> supplier, BiConsumer<E, ? super I> accumulator) {
         this.supplier = supplier;
         this.accumulator = accumulator;
      }

      @Override
      public CompletionStage<E> apply(Publisher<I> iPublisher) {
         return Flowable.fromPublisher(iPublisher)
               .collect(supplier::get, accumulator::accept)
               .toCompletionStage();
      }
   }

   private static final class CollectorReducer<I, E> implements Function<Publisher<I>, CompletionStage<E>> {
      private final Collector<? super I, E, ?> collector;

      private CollectorReducer(Collector<? super I, E, ?> collector) {
         this.collector = collector;
      }

      @Override
      public CompletionStage<E> apply(Publisher<I> iPublisher) {
         return Flowable.fromPublisher(iPublisher)
               .collect(collector.supplier()::get, collector.accumulator()::accept)
               .toCompletionStage();
      }
   }

   private static final class CombinerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {
      private final BiConsumer<E, E> biConsumer;

      private CombinerFinalizer(BiConsumer<E, E> biConsumer) {
         this.biConsumer = biConsumer;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce((e1, e2) -> {
                  biConsumer.accept(e1, e2);
                  return e1;
               })
               .toCompletionStage(null);
      }
   }

   private static final class FindFirstReducerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {
      private static final FindFirstReducerFinalizer INSTANCE = new FindFirstReducerFinalizer();

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .firstElement()
               .toCompletionStage(null);
      }
   }

   private static class MaxReducerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {
      private final Comparator<? super E> comparator;

      private MaxReducerFinalizer(Comparator<? super E> comparator) {

         this.comparator = comparator;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce((e1, e2) -> {
                  if (comparator.compare(e1, e2) > 0) {
                     return e1;
                  }
                  return e2;
               })
               .toCompletionStage(null);
      }
   }

   private static class MinReducerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {
      private final Comparator<? super E> comparator;

      private MinReducerFinalizer(Comparator<? super E> comparator) {

         this.comparator = comparator;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce((e1, e2) -> {
                  if (comparator.compare(e1, e2) > 0) {
                     return e2;
                  }
                  return e1;
               })
               .toCompletionStage(null);
      }
   }

   private static class NoneMatchReducer<E> implements Function<Publisher<E>, CompletionStage<Boolean>> {
      private final Predicate<? super E> predicate;

      private NoneMatchReducer(Predicate<? super E> predicate) {

         this.predicate = predicate;
      }

      @Override
      public CompletionStage<Boolean> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .all(predicate.negate()::test)
               .toCompletionStage();
      }
   }

   private static final class OrFinalizer implements Function<Publisher<Boolean>, CompletionStage<Boolean>> {
      private static final OrFinalizer INSTANCE = new OrFinalizer();

      @Override
      public CompletionStage<Boolean> apply(Publisher<Boolean> booleanPublisher) {
         return Flowable.fromPublisher(booleanPublisher)
               .any(bool -> bool == Boolean.TRUE)
               .toCompletionStage();
      }
   }

   private static class ReduceWithIdentityReducer<I, E> implements Function<Publisher<I>, CompletionStage<E>> {
      private final E identity;
      private final BiFunction<E, ? super I, E> biFunction;

      private ReduceWithIdentityReducer(E identity, BiFunction<E, ? super I, E> biFunction) {
         this.identity = identity;
         this.biFunction = biFunction;
      }

      @Override
      public CompletionStage<E> apply(Publisher<I> iPublisher) {
         return Flowable.fromPublisher(iPublisher)
               .reduce(identity, biFunction::apply)
               .toCompletionStage();
      }
   }

   private static class ReduceWithInitialSupplierReducer<I, E> implements Function<Publisher<I>, CompletionStage<E>> {
      private final Callable<? extends E> initialSupplier;
      private final BiFunction<E, ? super I, E> biFunction;

      private ReduceWithInitialSupplierReducer(Callable<? extends E> initialSupplier, BiFunction<E, ? super I, E> biFunction) {
         this.initialSupplier = initialSupplier;
         this.biFunction = biFunction;
      }

      @Override
      public CompletionStage<E> apply(Publisher<I> iPublisher) {
         return Flowable.fromPublisher(iPublisher)
               .reduceWith(initialSupplier::call, biFunction::apply)
               .toCompletionStage();
      }
   }

   private static class ReduceReducerFinalizer<E> implements Function<Publisher<E>, CompletionStage<E>> {
      private final BinaryOperator<E> operator;

      private ReduceReducerFinalizer(BinaryOperator<E> operator) {
         this.operator = operator;
      }

      @Override
      public CompletionStage<E> apply(Publisher<E> ePublisher) {
         return Flowable.fromPublisher(ePublisher)
               .reduce(operator::apply)
               .toCompletionStage(null);
      }
   }

   private static class SumReducer implements Function<Publisher<?>, CompletionStage<Long>> {
      private static final SumReducer INSTANCE = new SumReducer();

      @Override
      public CompletionStage<Long> apply(Publisher<?> longPublisher) {
         return Flowable.fromPublisher(longPublisher)
               .count()
               .toCompletionStage();
      }
   }

   private static class SumFinalizer implements Function<Publisher<Long>, CompletionStage<Long>> {
      private static final SumFinalizer INSTANCE = new SumFinalizer();

      @Override
      public CompletionStage<Long> apply(Publisher<Long> longPublisher) {
         return Flowable.fromPublisher(longPublisher)
               .reduce((long) 0, Long::sum)
               .toCompletionStage();
      }
   }

   private static class ToArrayReducer<I extends E, E> implements Function<Publisher<I>, CompletionStage<E[]>> {
      private final IntFunction<E[]> generator;

      private ToArrayReducer(IntFunction<E[]> generator) {
         this.generator = generator;
      }

      @Override
      public CompletionStage<E[]> apply(Publisher<I> ePublisher) {
         Single<List<I>> listSingle = Flowable.fromPublisher(ePublisher).toList();
         Single<E[]> arraySingle;
         if (generator != null) {
            arraySingle = listSingle.map(l -> {
               E[] array = generator.apply(l.size());
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

   private static class ToArrayFinalizer<E> implements Function<Publisher<E[]>, CompletionStage<E[]>> {
      private final IntFunction<E[]> generator;

      private ToArrayFinalizer(IntFunction<E[]> generator) {
         this.generator = generator;
      }

      @Override
      public CompletionStage<E[]> apply(Publisher<E[]> publisher) {
         Flowable<E[]> flowable = Flowable.fromPublisher(publisher);
         Single<E[]> arraySingle;
         if (generator != null) {
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

   public static final class PublisherReducersExternalizer implements AdvancedExternalizer<Object> {
      enum ExternalizerId {
         ALL_MATCH_REDUCER(AllMatchReducer.class),
         ANY_MATCH_REDUCER(AnyMatchReducer.class),
         AND_FINALIZER(AndFinalizer.class),
         COLLECT_REDUCER(CollectReducer.class),
         COLLECTOR_FINALIZER(CollectorFinalizer.class),
         COLLECTOR_REDUCER(CollectorReducer.class),
         COMBINER_FINALIZER(CombinerFinalizer.class),
         FIND_FIRST_REDUCER_FINALIZER(FindFirstReducerFinalizer.class),
         MAX_REDUCER_FINALIZER(MaxReducerFinalizer.class),
         MIN_REDUCER_FINALIZER(MinReducerFinalizer.class),
         NONE_MATCH_REDUCER(NoneMatchReducer.class),
         OR_FINALIZER(OrFinalizer.class),
         REDUCE_WITH_IDENTITY_REDUCER(ReduceWithIdentityReducer.class),
         REDUCE_WITH_INITIAL_SUPPLIER_REDUCER(ReduceWithInitialSupplierReducer.class),
         REDUCE_REDUCER_FINALIZER(ReduceReducerFinalizer.class),
         SUM_REDUCER(SumReducer.class),
         SUM_FINALIZER(SumFinalizer.class),
         TO_ARRAY_FINALIZER(ToArrayFinalizer.class),
         TO_ARRAY_REDUCER(ToArrayReducer.class),
         ;

         private final Class<?> marshalledClass;

         ExternalizerId(Class<?> marshalledClass) {
            this.marshalledClass = marshalledClass;
         }
      }

      private static final ExternalizerId[] VALUES = ExternalizerId.values();

      private final Map<Class<?>, ExternalizerId> objects = new HashMap<>();

      public PublisherReducersExternalizer() {
         for (ExternalizerId id : ExternalizerId.values()) {
            objects.put(id.marshalledClass, id);
         }
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return objects.keySet();
      }

      @Override
      public Integer getId() {
         return Ids.PUBLISHER_REDUCERS;
      }

      @Override
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ExternalizerId id = objects.get(object.getClass());
         if (id == null) {
            throw new IllegalArgumentException("Unsupported class " + object.getClass() + " was provided!");
         }
         output.writeByte(id.ordinal());
         switch (id) {
            case ALL_MATCH_REDUCER:
               output.writeObject(((AllMatchReducer) object).predicate);
               break;
            case ANY_MATCH_REDUCER:
               output.writeObject(((AnyMatchReducer) object).predicate);
               break;
            case COLLECT_REDUCER:
               output.writeObject(((CollectReducer) object).supplier);
               output.writeObject(((CollectReducer) object).accumulator);
               break;
            case COLLECTOR_FINALIZER:
               output.writeObject(((CollectorFinalizer) object).collector);
               break;
            case COLLECTOR_REDUCER:
               output.writeObject(((CollectorReducer) object).collector);
               break;
            case COMBINER_FINALIZER:
               output.writeObject(((CombinerFinalizer) object).biConsumer);
               break;
            case MAX_REDUCER_FINALIZER:
               output.writeObject(((MaxReducerFinalizer) object).comparator);
               break;
            case MIN_REDUCER_FINALIZER:
               output.writeObject(((MinReducerFinalizer) object).comparator);
               break;
            case NONE_MATCH_REDUCER:
               output.writeObject(((NoneMatchReducer) object).predicate);
               break;
            case REDUCE_WITH_IDENTITY_REDUCER:
               output.writeObject(((ReduceWithIdentityReducer) object).identity);
               output.writeObject(((ReduceWithIdentityReducer) object).biFunction);
               break;
            case REDUCE_WITH_INITIAL_SUPPLIER_REDUCER:
               output.writeObject(((ReduceWithInitialSupplierReducer) object).initialSupplier);
               output.writeObject(((ReduceWithInitialSupplierReducer) object).biFunction);
               break;
            case REDUCE_REDUCER_FINALIZER:
               output.writeObject(((ReduceReducerFinalizer) object).operator);
               break;
            case TO_ARRAY_REDUCER:
               output.writeObject(((ToArrayReducer) object).generator);
               break;
            case TO_ARRAY_FINALIZER:
               output.writeObject(((ToArrayFinalizer) object).generator);
               break;
         }
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readUnsignedByte();
         ExternalizerId[] ids = VALUES;
         if (number < 0 || number >= ids.length) {
            throw new IllegalArgumentException("Found invalid number " + number);
         }
         ExternalizerId id = ids[number];
         switch (id) {
            case AND_FINALIZER:
               return AndFinalizer.INSTANCE;
            case ALL_MATCH_REDUCER:
               return new AllMatchReducer((Predicate) input.readObject());
            case ANY_MATCH_REDUCER:
               return new AnyMatchReducer((Predicate) input.readObject());
            case COLLECT_REDUCER:
               return new CollectReducer((Supplier) input.readObject(), (BiConsumer) input.readObject());
            case COLLECTOR_FINALIZER:
               return new CollectorFinalizer((Collector) input.readObject());
            case COLLECTOR_REDUCER:
               return new CollectorReducer((Collector) input.readObject());
            case COMBINER_FINALIZER:
               return new CombinerFinalizer((BiConsumer) input.readObject());
            case FIND_FIRST_REDUCER_FINALIZER:
               return FindFirstReducerFinalizer.INSTANCE;
            case MAX_REDUCER_FINALIZER:
               return new MaxReducerFinalizer<>((Comparator) input.readObject());
            case MIN_REDUCER_FINALIZER:
               return new MinReducerFinalizer((Comparator) input.readObject());
            case NONE_MATCH_REDUCER:
               return new NoneMatchReducer((Predicate) input.readObject());
            case OR_FINALIZER:
               return OrFinalizer.INSTANCE;
            case REDUCE_WITH_IDENTITY_REDUCER:
               return new ReduceWithIdentityReducer(input.readObject(), (BiFunction) input.readObject());
            case REDUCE_WITH_INITIAL_SUPPLIER_REDUCER:
               return new ReduceWithInitialSupplierReducer<>((Callable) input.readObject(), (BiFunction) input.readObject());
            case REDUCE_REDUCER_FINALIZER:
               return new ReduceReducerFinalizer((BinaryOperator) input.readObject());
            case SUM_REDUCER:
               return SumReducer.INSTANCE;
            case SUM_FINALIZER:
               return SumFinalizer.INSTANCE;
            case TO_ARRAY_REDUCER:
               return new ToArrayReducer((IntFunction) input.readObject());
            case TO_ARRAY_FINALIZER:
               return new ToArrayFinalizer((IntFunction) input.readObject());
            default:
               throw new IllegalArgumentException("ExternalizerId not supported: " + id);
         }
      }
   }
}
