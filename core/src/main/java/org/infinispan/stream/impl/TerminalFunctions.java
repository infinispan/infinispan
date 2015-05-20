package org.infinispan.stream.impl;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Static factory class used to provide marshallable terminal operations
 */
final class TerminalFunctions {
   private TerminalFunctions() { }

   public static <T> Function<Stream<T>, Boolean> allMatchFunction(Predicate<? super T> predicate) {
      return new AllMatchFunction<>(predicate);
   }

   public static Function<DoubleStream, Boolean> allMatchFunction(DoublePredicate predicate) {
      return new AllMatchDoubleFunction<>(predicate);
   }

   public static Function<IntStream, Boolean> allMatchFunction(IntPredicate predicate) {
      return new AllMatchIntFunction<>(predicate);
   }

   public static Function<LongStream, Boolean> allMatchFunction(LongPredicate predicate) {
      return new AllMatchLongFunction<>(predicate);
   }

   public static <T> Function<Stream<T>, Boolean> anyMatchFunction(Predicate<? super T> predicate) {
      return new AnyMatchFunction<>(predicate);
   }

   public static Function<DoubleStream, Boolean> anyMatchFunction(DoublePredicate predicate) {
      return new AnyMatchDoubleFunction<>(predicate);
   }

   public static Function<IntStream, Boolean> anyMatchFunction(IntPredicate predicate) {
      return new AnyMatchIntFunction<>(predicate);
   }

   public static Function<LongStream, Boolean> anyMatchFunction(LongPredicate predicate) {
      return new AnyMatchLongFunction<>(predicate);
   }

   public static Function<DoubleStream, double[]> averageDoubleFunction() {
      return AverageDoubleFunction.getInstance();
   }

   public static Function<IntStream, long[]> averageIntFunction() {
      return AverageIntFunction.getInstance();
   }

   public static Function<LongStream, long[]> averageLongFunction() {
      return AverageLongFunction.getInstance();
   }

   public static <T, R> Function<Stream<T>, R> collectFunction(Supplier<R> supplier,
           BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
      return new CollectFunction<>(supplier, accumulator, combiner);
   }

   public static <R> Function<DoubleStream, R> collectFunction(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator,
           BiConsumer<R, R> combiner) {
      return new CollectDoubleFunction<>(supplier, accumulator, combiner);
   }

   public static <R> Function<IntStream, R> collectFunction(Supplier<R> supplier, ObjIntConsumer<R> accumulator,
           BiConsumer<R, R> combiner) {
      return new CollectIntFunction<>(supplier, accumulator, combiner);
   }

   public static <R> Function<LongStream, R> collectFunction(Supplier<R> supplier, ObjLongConsumer<R> accumulator,
           BiConsumer<R, R> combiner) {
      return new CollectLongFunction<>(supplier, accumulator, combiner);
   }

   public static <T, R> Function<Stream<T>, R> collectorFunction(Collector<? super T, ?, R> collector) {
      return new CollectorFunction<>(collector);
   }

   public static <T> Function<Stream<T>, Long> countFunction() {
      return CountFunction.getInstance();
   }

   public static Function<DoubleStream, Long> countDoubleFunction() {
      return CountDoubleFunction.getInstance();
   }

   public static Function<IntStream, Long> countIntFunction() {
      return CountIntFunction.getInstance();
   }

   public static Function<LongStream, Long> countLongFunction() {
      return CountLongFunction.getInstance();
   }

   public static <T> Function<Stream<T>, T> findAnyFunction() {
      return FindAnyFunction.getInstance();
   }

   public static Function<DoubleStream, Double> findAnyDoubleFunction() {
      return FindAnyDoubleFunction.getInstance();
   }

   public static Function<IntStream, Integer> findAnyIntFunction() {
      return FindAnyIntFunction.getInstance();
   }

   public static Function<LongStream, Long> findAnyLongFunction() {
      return FindAnyLongFunction.getInstance();
   }

   public static <T> Function<Stream<T>, Void> forEachFunction(Consumer<? super T> consumer) {
      return new ForEachFunction<>(consumer);
   }

   public static Function<DoubleStream, Void> forEachFunction(DoubleConsumer consumer) {
      return new ForEachDoubleFunction<>(consumer);
   }

   public static Function<IntStream, Void> forEachFunction(IntConsumer consumer) {
      return new ForEachIntFunction<>(consumer);
   }

   public static Function<LongStream, Void> forEachFunction(LongConsumer consumer) {
      return new ForEachLongFunction<>(consumer);
   }

   public static <T> Function<Stream<T>, T> maxFunction(Comparator<? super T> comparator) {
      return new MaxFunction<>(comparator);
   }

   public static Function<DoubleStream, Double> maxDoubleFunction() {
      return MaxDoubleFunction.getInstance();
   }

   public static Function<IntStream, Integer> maxIntFunction() {
      return MaxIntFunction.getInstance();
   }

   public static Function<LongStream, Long> maxLongFunction() {
      return MaxLongFunction.getInstance();
   }

   public static <T> Function<Stream<T>, T> minFunction(Comparator<? super T> comparator) {
      return new MinFunction<>(comparator);
   }

   public static Function<DoubleStream, Double> minDoubleFunction() {
      return MinDoubleFunction.getInstance();
   }

   public static Function<IntStream, Integer> minIntFunction() {
      return MinIntFunction.getInstance();
   }

   public static Function<LongStream, Long> minLongFunction() {
      return MinLongFunction.getInstance();
   }

   public static <T> Function<Stream<T>, Boolean> noneMatchFunction(Predicate<? super T> predicate) {
      return new NoneMatchFunction<>(predicate);
   }

   public static Function<DoubleStream, Boolean> noneMatchFunction(DoublePredicate predicate) {
      return new NoneMatchDoubleFunction<>(predicate);
   }

   public static Function<IntStream, Boolean> noneMatchFunction(IntPredicate predicate) {
      return new NoneMatchIntFunction<>(predicate);
   }

   public static Function<LongStream, Boolean> noneMatchFunction(LongPredicate predicate) {
      return new NoneMatchLongFunction<>(predicate);
   }

   public static <T> Function<Stream<T>, T> reduceFunction(BinaryOperator<T> accumulator) {
      return new ReduceFunction<>(accumulator);
   }

   public static Function<DoubleStream, Double> reduceFunction(DoubleBinaryOperator accumulator) {
      return new ReduceDoubleFunction<>(accumulator);
   }

   public static Function<IntStream, Integer> reduceFunction(IntBinaryOperator accumulator) {
      return new ReduceIntFunction<>(accumulator);
   }

   public static Function<LongStream, Long> reduceFunction(LongBinaryOperator accumulator) {
      return new ReduceLongFunction<>(accumulator);
   }

   public static <T> Function<Stream<T>, T> reduceFunction(T identity, BinaryOperator<T> accumulator) {
      return new IdentityReduceFunction<>(identity, accumulator);
   }

   public static Function<DoubleStream, Double> reduceFunction(double identity, DoubleBinaryOperator accumulator) {
      return new IdentityReduceDoubleFunction<>(identity, accumulator);
   }

   public static Function<IntStream, Integer> reduceFunction(int identity, IntBinaryOperator accumulator) {
      return new IdentityReduceIntFunction<>(identity, accumulator);
   }

   public static Function<LongStream, Long> reduceFunction(long identity, LongBinaryOperator accumulator) {
      return new IdentityReduceLongFunction<>(identity, accumulator);
   }

   public static <T, R> Function<Stream<T>, R> reduceFunction(R identity, BiFunction<R, ? super T, R> accumulator,
           BinaryOperator<R> combiner) {
      return new IdentityReduceCombinerFunction<>(identity, accumulator, combiner);
   }

   public static Function<DoubleStream, Double> sumDoubleFunction() {
      return SumDoubleFunction.getInstance();
   }

   public static Function<IntStream, Integer> sumIntFunction() {
      return SumIntFunction.getInstance();
   }

   public static Function<LongStream, Long> sumLongFunction() {
      return SumLongFunction.getInstance();
   }

   public static <T> Function<Stream<T>, Object[]> toArrayFunction() {
      return ToArrayFunction.getInstance();
   }

   public static Function<DoubleStream, double[]> toArrayDoubleFunction() {
      return ToArrayDoubleFunction.getInstance();
   }

   public static Function<IntStream, int[]> toArrayIntFunction() {
      return ToArrayIntFunction.getInstance();
   }

   public static Function<LongStream, long[]> toArrayLongFunction() {
      return ToArrayLongFunction.getInstance();
   }

   public static <T, R> Function<Stream<R>, T[]> toArrayFunction(IntFunction<T[]> generator) {
      return new ToArrayGeneratorFunction<>(generator);
   }

   @SerializeWith(value = AllMatchFunction.AllMatchFunctionExternalizer.class)
   private static final class AllMatchFunction<T> implements Function<Stream<T>, Boolean> {
      private final Predicate<? super T> predicate;

      private AllMatchFunction(Predicate<? super T> predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(Stream<T> stream) {
         return stream.allMatch(predicate);
      }

      public static final class AllMatchFunctionExternalizer implements Externalizer<AllMatchFunction> {
         @Override
         public void writeObject(ObjectOutput output, AllMatchFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public AllMatchFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new AllMatchFunction((Predicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = AllMatchDoubleFunction.AllMatchDoubleFunctionExternalizer.class)
   private static final class AllMatchDoubleFunction<T> implements Function<DoubleStream, Boolean> {
      private final DoublePredicate predicate;

      private AllMatchDoubleFunction(DoublePredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(DoubleStream stream) {
         return stream.allMatch(predicate);
      }

      public static final class AllMatchDoubleFunctionExternalizer implements Externalizer<AllMatchDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, AllMatchDoubleFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public AllMatchDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new AllMatchDoubleFunction((DoublePredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = AllMatchIntFunction.AllMatchIntFunctionExternalizer.class)
   private static final class AllMatchIntFunction<T> implements Function<IntStream, Boolean> {
      private final IntPredicate predicate;

      private AllMatchIntFunction(IntPredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(IntStream stream) {
         return stream.allMatch(predicate);
      }

      public static final class AllMatchIntFunctionExternalizer implements Externalizer<AllMatchIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, AllMatchIntFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public AllMatchIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new AllMatchIntFunction((IntPredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = AllMatchLongFunction.AllMatchLongFunctionExternalizer.class)
   private static final class AllMatchLongFunction<T> implements Function<LongStream, Boolean> {
      private final LongPredicate predicate;

      private AllMatchLongFunction(LongPredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(LongStream stream) {
         return stream.allMatch(predicate);
      }

      public static final class AllMatchLongFunctionExternalizer implements Externalizer<AllMatchLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, AllMatchLongFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public AllMatchLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new AllMatchLongFunction((LongPredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = AnyMatchFunction.AnyMatchFunctionExternalizer.class)
   private static final class AnyMatchFunction<T> implements Function<Stream<T>, Boolean> {
      private final Predicate<? super T> predicate;

      private AnyMatchFunction(Predicate<? super T> predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(Stream<T> stream) {
         return stream.anyMatch(predicate);
      }

      public static final class AnyMatchFunctionExternalizer implements Externalizer<AnyMatchFunction> {
         @Override
         public void writeObject(ObjectOutput output, AnyMatchFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public AnyMatchFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new AnyMatchFunction((Predicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = AnyMatchDoubleFunction.AnyMatchDoubleFunctionExternalizer.class)
   private static final class AnyMatchDoubleFunction<T> implements Function<DoubleStream, Boolean> {
      private final DoublePredicate predicate;

      private AnyMatchDoubleFunction(DoublePredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(DoubleStream stream) {
         return stream.anyMatch(predicate);
      }

      public static final class AnyMatchDoubleFunctionExternalizer implements Externalizer<AnyMatchDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, AnyMatchDoubleFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public AnyMatchDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new AnyMatchDoubleFunction((DoublePredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = AnyMatchIntFunction.AnyMatchIntFunctionExternalizer.class)
   private static final class AnyMatchIntFunction<T> implements Function<IntStream, Boolean> {
      private final IntPredicate predicate;

      private AnyMatchIntFunction(IntPredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(IntStream stream) {
         return stream.anyMatch(predicate);
      }

      public static final class AnyMatchIntFunctionExternalizer implements Externalizer<AnyMatchIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, AnyMatchIntFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public AnyMatchIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new AnyMatchIntFunction((IntPredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = AnyMatchLongFunction.AnyMatchLongFunctionExternalizer.class)
   private static final class AnyMatchLongFunction<T> implements Function<LongStream, Boolean> {
      private final LongPredicate predicate;

      private AnyMatchLongFunction(LongPredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(LongStream stream) {
         return stream.anyMatch(predicate);
      }

      public static final class AnyMatchLongFunctionExternalizer implements Externalizer<AnyMatchLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, AnyMatchLongFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public AnyMatchLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new AnyMatchLongFunction((LongPredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = AverageDoubleFunction.AverageDoubleFunctionExternalizer.class)
   private static final class AverageDoubleFunction implements Function<DoubleStream, double[]> {
      private static final AverageDoubleFunction OPERATION = new AverageDoubleFunction();

      private AverageDoubleFunction() { }

      public static AverageDoubleFunction getInstance() {
         return OPERATION;
      }

      @Override
      public double[] apply(DoubleStream stream) {
         DoubleSummaryStatistics stats = stream.summaryStatistics();
         return new double[]{stats.getSum(), stats.getCount()};
      }

      public static final class AverageDoubleFunctionExternalizer implements Externalizer<AverageDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, AverageDoubleFunction object) throws IOException {
         }

         @Override
         public AverageDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = AverageIntFunction.AverageIntFunctionExternalizer.class)
   private static final class AverageIntFunction implements Function<IntStream, long[]> {
      private static final AverageIntFunction OPERATION = new AverageIntFunction();

      private AverageIntFunction() { }

      public static AverageIntFunction getInstance() {
         return OPERATION;
      }

      @Override
      public long[] apply(IntStream stream) {
         IntSummaryStatistics stats = stream.summaryStatistics();
         return new long[]{stats.getSum(), stats.getCount()};
      }

      public static final class AverageIntFunctionExternalizer implements Externalizer<AverageIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, AverageIntFunction object) throws IOException {
         }

         @Override
         public AverageIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = AverageLongFunction.AverageLongFunctionExternalizer.class)
   private static final class AverageLongFunction implements Function<LongStream, long[]> {
      private static final AverageLongFunction OPERATION = new AverageLongFunction();

      private AverageLongFunction() { }

      public static AverageLongFunction getInstance() {
         return OPERATION;
      }

      @Override
      public long[] apply(LongStream stream) {
         LongSummaryStatistics stats = stream.summaryStatistics();
         return new long[]{stats.getSum(), stats.getCount()};
      }

      public static final class AverageLongFunctionExternalizer implements Externalizer<AverageLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, AverageLongFunction object) throws IOException {
         }

         @Override
         public AverageLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = CountFunction.CountFunctionExternalizer.class)
   private static final class CountFunction<T> implements Function<Stream<T>, Long> {
      private static final CountFunction<?> OPERATION = new CountFunction<>();

      private CountFunction() { }

      public static <S> CountFunction<S> getInstance() {
         return (CountFunction<S>) OPERATION;
      }

      @Override
      public Long apply(Stream<T> stream) {
         return stream.count();
      }

      public static final class CountFunctionExternalizer implements Externalizer<CountFunction> {
         @Override
         public void writeObject(ObjectOutput output, CountFunction object) throws IOException {
         }

         @Override
         public CountFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = CountDoubleFunction.CountDoubleFunctionExternalizer.class)
   private static final class CountDoubleFunction<T> implements Function<DoubleStream, Long> {
      private static final CountDoubleFunction<?> OPERATION = new CountDoubleFunction<>();

      private CountDoubleFunction() { }

      public static <S> CountDoubleFunction<S> getInstance() {
         return (CountDoubleFunction<S>) OPERATION;
      }

      @Override
      public Long apply(DoubleStream stream) {
         return stream.count();
      }

      public static final class CountDoubleFunctionExternalizer implements Externalizer<CountDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, CountDoubleFunction object) throws IOException {
         }

         @Override
         public CountDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = CountIntFunction.CountIntFunctionExternalizer.class)
   private static final class CountIntFunction<T> implements Function<IntStream, Long> {
      private static final CountIntFunction<?> OPERATION = new CountIntFunction<>();

      private CountIntFunction() { }

      public static <S> CountIntFunction<S> getInstance() {
         return (CountIntFunction<S>) OPERATION;
      }

      @Override
      public Long apply(IntStream stream) {
         return stream.count();
      }

      public static final class CountIntFunctionExternalizer implements Externalizer<CountIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, CountIntFunction object) throws IOException {
         }

         @Override
         public CountIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = CountLongFunction.CountLongFunctionExternalizer.class)
   private static final class CountLongFunction<T> implements Function<LongStream, Long> {
      private static final CountLongFunction<?> OPERATION = new CountLongFunction<>();

      private CountLongFunction() { }

      public static <S> CountLongFunction<S> getInstance() {
         return (CountLongFunction<S>) OPERATION;
      }

      @Override
      public Long apply(LongStream stream) {
         return stream.count();
      }

      public static final class CountLongFunctionExternalizer implements Externalizer<CountLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, CountLongFunction object) throws IOException {
         }

         @Override
         public CountLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = FindAnyFunction.FindAnyFunctionExternalizer.class)
   private static final class FindAnyFunction<T> implements Function<Stream<T>, T> {
      private static final FindAnyFunction<?> OPERATION = new FindAnyFunction<>();

      private FindAnyFunction() { }

      public static <S> FindAnyFunction<S> getInstance() {
         return (FindAnyFunction<S>) OPERATION;
      }

      @Override
      public T apply(Stream<T> stream) {
         return stream.findAny().orElse(null);
      }

      public static final class FindAnyFunctionExternalizer implements Externalizer<FindAnyFunction> {
         @Override
         public void writeObject(ObjectOutput output, FindAnyFunction object) throws IOException {
         }

         @Override
         public FindAnyFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = FindAnyDoubleFunction.FindAnyDoubleFunctionExternalizer.class)
   private static final class FindAnyDoubleFunction implements Function<DoubleStream, Double> {
      private static final FindAnyDoubleFunction OPERATION = new FindAnyDoubleFunction();

      private FindAnyDoubleFunction() { }

      public static FindAnyDoubleFunction getInstance() {
         return (FindAnyDoubleFunction) OPERATION;
      }

      @Override
      public Double apply(DoubleStream stream) {
         OptionalDouble i = stream.findAny();
         if (i.isPresent()) {
            return i.getAsDouble();
         } else {
            return null;
         }
      }

      public static final class FindAnyDoubleFunctionExternalizer implements Externalizer<FindAnyDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, FindAnyDoubleFunction object) throws IOException {
         }

         @Override
         public FindAnyDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = FindAnyIntFunction.FindAnyIntFunctionExternalizer.class)
   private static final class FindAnyIntFunction implements Function<IntStream, Integer> {
      private static final FindAnyIntFunction OPERATION = new FindAnyIntFunction();

      private FindAnyIntFunction() { }

      public static FindAnyIntFunction getInstance() {
         return (FindAnyIntFunction) OPERATION;
      }

      @Override
      public Integer apply(IntStream stream) {
         OptionalInt i = stream.findAny();
         if (i.isPresent()) {
            return i.getAsInt();
         } else {
            return null;
         }
      }

      public static final class FindAnyIntFunctionExternalizer implements Externalizer<FindAnyIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, FindAnyIntFunction object) throws IOException {
         }

         @Override
         public FindAnyIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = FindAnyLongFunction.FindAnyLongFunctionExternalizer.class)
   private static final class FindAnyLongFunction implements Function<LongStream, Long> {
      private static final FindAnyLongFunction OPERATION = new FindAnyLongFunction();

      private FindAnyLongFunction() { }

      public static FindAnyLongFunction getInstance() {
         return (FindAnyLongFunction) OPERATION;
      }

      @Override
      public Long apply(LongStream stream) {
         OptionalLong i = stream.findAny();
         if (i.isPresent()) {
            return i.getAsLong();
         } else {
            return null;
         }
      }

      public static final class FindAnyLongFunctionExternalizer implements Externalizer<FindAnyLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, FindAnyLongFunction object) throws IOException {
         }

         @Override
         public FindAnyLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return getInstance();
         }
      }
   }

   @SerializeWith(value = NoneMatchFunction.NoneMatchFunctionExternalizer.class)
   private static final class NoneMatchFunction<T> implements Function<Stream<T>, Boolean> {
      private final Predicate<? super T> predicate;

      private NoneMatchFunction(Predicate<? super T> predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(Stream<T> stream) {
         return stream.noneMatch(predicate);
      }

      public static final class NoneMatchFunctionExternalizer implements Externalizer<NoneMatchFunction> {
         @Override
         public void writeObject(ObjectOutput output, NoneMatchFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public NoneMatchFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new NoneMatchFunction((Predicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = NoneMatchDoubleFunction.NoneMatchDoubleFunctionExternalizer.class)
   private static final class NoneMatchDoubleFunction<T> implements Function<DoubleStream, Boolean> {
      private final DoublePredicate predicate;

      private NoneMatchDoubleFunction(DoublePredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(DoubleStream stream) {
         return stream.noneMatch(predicate);
      }

      public static final class NoneMatchDoubleFunctionExternalizer implements Externalizer<NoneMatchDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, NoneMatchDoubleFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public NoneMatchDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new NoneMatchDoubleFunction((DoublePredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = NoneMatchIntFunction.NoneMatchIntFunctionExternalizer.class)
   private static final class NoneMatchIntFunction<T> implements Function<IntStream, Boolean> {
      private final IntPredicate predicate;

      private NoneMatchIntFunction(IntPredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(IntStream stream) {
         return stream.noneMatch(predicate);
      }

      public static final class NoneMatchIntFunctionExternalizer implements Externalizer<NoneMatchIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, NoneMatchIntFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public NoneMatchIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new NoneMatchIntFunction((IntPredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = NoneMatchLongFunction.NoneMatchLongFunctionExternalizer.class)
   private static final class NoneMatchLongFunction<T> implements Function<LongStream, Boolean> {
      private final LongPredicate predicate;

      private NoneMatchLongFunction(LongPredicate predicate) {
         this.predicate = predicate;
      }

      @Override
      public Boolean apply(LongStream stream) {
         return stream.noneMatch(predicate);
      }

      public static final class NoneMatchLongFunctionExternalizer implements Externalizer<NoneMatchLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, NoneMatchLongFunction object) throws IOException {
            output.writeObject(object.predicate);
         }

         @Override
         public NoneMatchLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new NoneMatchLongFunction((LongPredicate) input.readObject());
         }
      }
   }

   @SerializeWith(value = CollectFunction.CollectFunctionExternalizer.class)
   private static final class CollectFunction<T, R> implements Function<Stream<T>, R> {
      private final Supplier<R> supplier;
      private final BiConsumer<R, ? super T> accumulator;
      private final BiConsumer<R, R> combiner;

      private CollectFunction(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
         this.supplier = supplier;
         this.accumulator = accumulator;
         this.combiner = combiner;
      }

      @Override
      public R apply(Stream<T> stream) {
         return stream.collect(supplier, accumulator, combiner);
      }

      public static final class CollectFunctionExternalizer implements Externalizer<CollectFunction> {

         @Override
         public void writeObject(ObjectOutput output, CollectFunction object) throws IOException {
            output.writeObject(object.supplier);
            output.writeObject(object.accumulator);
            output.writeObject(object.combiner);
         }

         @Override
         public CollectFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new CollectFunction((Supplier) input.readObject(), (BiConsumer) input.readObject(),
                    (BiConsumer) input.readObject());
         }
      }
   }

   @SerializeWith(value = CollectDoubleFunction.CollectDoubleFunctionExternalizer.class)
   private static final class CollectDoubleFunction<T, R> implements Function<DoubleStream, R> {
      private final Supplier<R> supplier;
      private final ObjDoubleConsumer<R> accumulator;
      private final BiConsumer<R, R> combiner;

      private CollectDoubleFunction(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
         this.supplier = supplier;
         this.accumulator = accumulator;
         this.combiner = combiner;
      }

      @Override
      public R apply(DoubleStream stream) {
         return stream.collect(supplier, accumulator, combiner);
      }

      public static final class CollectDoubleFunctionExternalizer implements Externalizer<CollectDoubleFunction> {

         @Override
         public void writeObject(ObjectOutput output, CollectDoubleFunction object) throws IOException {
            output.writeObject(object.supplier);
            output.writeObject(object.accumulator);
            output.writeObject(object.combiner);
         }

         @Override
         public CollectDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new CollectDoubleFunction((Supplier) input.readObject(), (ObjDoubleConsumer) input.readObject(),
                    (BiConsumer) input.readObject());
         }
      }
   }

   @SerializeWith(value = CollectIntFunction.CollectIntFunctionExternalizer.class)
   private static final class CollectIntFunction<T, R> implements Function<IntStream, R> {
      private final Supplier<R> supplier;
      private final ObjIntConsumer<R> accumulator;
      private final BiConsumer<R, R> combiner;

      private CollectIntFunction(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
         this.supplier = supplier;
         this.accumulator = accumulator;
         this.combiner = combiner;
      }

      @Override
      public R apply(IntStream stream) {
         return stream.collect(supplier, accumulator, combiner);
      }

      public static final class CollectIntFunctionExternalizer implements Externalizer<CollectIntFunction> {

         @Override
         public void writeObject(ObjectOutput output, CollectIntFunction object) throws IOException {
            output.writeObject(object.supplier);
            output.writeObject(object.accumulator);
            output.writeObject(object.combiner);
         }

         @Override
         public CollectIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new CollectIntFunction((Supplier) input.readObject(), (ObjIntConsumer) input.readObject(),
                    (BiConsumer) input.readObject());
         }
      }
   }

   @SerializeWith(value = CollectLongFunction.CollectLongFunctionExternalizer.class)
   private static final class CollectLongFunction<T, R> implements Function<LongStream, R> {
      private final Supplier<R> supplier;
      private final ObjLongConsumer<R> accumulator;
      private final BiConsumer<R, R> combiner;

      private CollectLongFunction(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
         this.supplier = supplier;
         this.accumulator = accumulator;
         this.combiner = combiner;
      }

      @Override
      public R apply(LongStream stream) {
         return stream.collect(supplier, accumulator, combiner);
      }

      public static final class CollectLongFunctionExternalizer implements Externalizer<CollectLongFunction> {

         @Override
         public void writeObject(ObjectOutput output, CollectLongFunction object) throws IOException {
            output.writeObject(object.supplier);
            output.writeObject(object.accumulator);
            output.writeObject(object.combiner);
         }

         @Override
         public CollectLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new CollectLongFunction((Supplier) input.readObject(), (ObjLongConsumer) input.readObject(),
                    (BiConsumer) input.readObject());
         }
      }
   }

   @SerializeWith(value = CollectorFunction.CollectorFunctionExternalizer.class)
   private static final class CollectorFunction<T, R> implements Function<Stream<T>, R> {
      private final Collector<? super T, ?, R> collector;

      private CollectorFunction(Collector<? super T, ?, R> collector) {
         this.collector = collector;
      }

      @Override
      public R apply(Stream<T> stream) {
         return stream.collect(collector);
      }

      public static final class CollectorFunctionExternalizer implements Externalizer<CollectorFunction> {
         @Override
         public void writeObject(ObjectOutput output, CollectorFunction object) throws IOException {
            output.writeObject(object.collector);
         }

         @Override
         public CollectorFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new CollectorFunction((Collector) input.readObject());
         }
      }
   }

   @SerializeWith(value = ForEachFunction.ForEachFunctionExternalizer.class)
   private static final class ForEachFunction<T> implements Function<Stream<T>, Void> {
      private final Consumer<? super T> consumer;

      private ForEachFunction(Consumer<? super T> consumer) {
         this.consumer = consumer;
      }

      @Override
      public Void apply(Stream<T> stream) {
         stream.forEach(consumer);
         return null;
      }

      public static final class ForEachFunctionExternalizer implements Externalizer<ForEachFunction> {
         @Override
         public void writeObject(ObjectOutput output, ForEachFunction object) throws IOException {
            output.writeObject(object.consumer);
         }

         @Override
         public ForEachFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ForEachFunction((Consumer) input.readObject());
         }
      }
   }

   @SerializeWith(value = ForEachDoubleFunction.ForEachDoubleFunctionExternalizer.class)
   private static final class ForEachDoubleFunction<T> implements Function<DoubleStream, Void> {
      private final DoubleConsumer consumer;

      private ForEachDoubleFunction(DoubleConsumer consumer) {
         this.consumer = consumer;
      }

      @Override
      public Void apply(DoubleStream stream) {
         stream.forEach(consumer);
         return null;
      }

      public static final class ForEachDoubleFunctionExternalizer implements Externalizer<ForEachDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, ForEachDoubleFunction object) throws IOException {
            output.writeObject(object.consumer);
         }

         @Override
         public ForEachDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ForEachDoubleFunction((DoubleConsumer) input.readObject());
         }
      }
   }

   @SerializeWith(value = ForEachIntFunction.ForEachIntFunctionExternalizer.class)
   private static final class ForEachIntFunction<T> implements Function<IntStream, Void> {
      private final IntConsumer consumer;

      private ForEachIntFunction(IntConsumer consumer) {
         this.consumer = consumer;
      }

      @Override
      public Void apply(IntStream stream) {
         stream.forEach(consumer);
         return null;
      }

      public static final class ForEachIntFunctionExternalizer implements Externalizer<ForEachIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, ForEachIntFunction object) throws IOException {
            output.writeObject(object.consumer);
         }

         @Override
         public ForEachIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ForEachIntFunction((IntConsumer) input.readObject());
         }
      }
   }

   @SerializeWith(value = ForEachLongFunction.ForEachLongFunctionExternalizer.class)
   private static final class ForEachLongFunction<T> implements Function<LongStream, Void> {
      private final LongConsumer consumer;

      private ForEachLongFunction(LongConsumer consumer) {
         this.consumer = consumer;
      }

      @Override
      public Void apply(LongStream stream) {
         stream.forEach(consumer);
         return null;
      }

      public static final class ForEachLongFunctionExternalizer implements Externalizer<ForEachLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, ForEachLongFunction object) throws IOException {
            output.writeObject(object.consumer);
         }

         @Override
         public ForEachLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ForEachLongFunction((LongConsumer) input.readObject());
         }
      }
   }

   @SerializeWith(value = MaxFunction.MaxFunctionExternalizer.class)
   private static final class MaxFunction<T> implements Function<Stream<T>, T> {
      private final Comparator<? super T> comparator;

      private MaxFunction(Comparator<? super T> comparator) {
         this.comparator = comparator;
      }

      @Override
      public T apply(Stream<T> stream) {
         return stream.max(comparator).orElse(null);
      }

      public static final class MaxFunctionExternalizer implements Externalizer<MaxFunction> {
         @Override
         public void writeObject(ObjectOutput output, MaxFunction object) throws IOException {
            output.writeObject(object.comparator);
         }

         @Override
         public MaxFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new MaxFunction((Comparator) input.readObject());
         }
      }
   }

   @SerializeWith(value = MaxDoubleFunction.MaxDoubleFunctionExternalizer.class)
   private static final class MaxDoubleFunction<T> implements Function<DoubleStream, Double> {
      private static final MaxDoubleFunction OPERATION = new MaxDoubleFunction();

      private MaxDoubleFunction() { }

      public static MaxDoubleFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Double apply(DoubleStream stream) {
         OptionalDouble op = stream.max();
         if (op.isPresent()) {
            return op.getAsDouble();
         } else {
            return null;
         }
      }

      public static final class MaxDoubleFunctionExternalizer implements Externalizer<MaxDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, MaxDoubleFunction object) throws IOException {
         }

         @Override
         public MaxDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return MaxDoubleFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = MaxIntFunction.MaxIntFunctionExternalizer.class)
   private static final class MaxIntFunction<T> implements Function<IntStream, Integer> {
      private static final MaxIntFunction OPERATION = new MaxIntFunction();

      private MaxIntFunction() { }

      public static MaxIntFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Integer apply(IntStream stream) {
         OptionalInt op = stream.max();
         if (op.isPresent()) {
            return op.getAsInt();
         } else {
            return null;
         }
      }

      public static final class MaxIntFunctionExternalizer implements Externalizer<MaxIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, MaxIntFunction object) throws IOException {
         }

         @Override
         public MaxIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return MaxIntFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = MaxLongFunction.MaxLongFunctionExternalizer.class)
   private static final class MaxLongFunction<T> implements Function<LongStream, Long> {
      private static final MaxLongFunction OPERATION = new MaxLongFunction();

      private MaxLongFunction() { }

      public static MaxLongFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Long apply(LongStream stream) {
         OptionalLong op = stream.max();
         if (op.isPresent()) {
            return op.getAsLong();
         } else {
            return null;
         }
      }

      public static final class MaxLongFunctionExternalizer implements Externalizer<MaxLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, MaxLongFunction object) throws IOException {
         }

         @Override
         public MaxLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return MaxLongFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = MinFunction.MinFunctionExternalizer.class)
   private static final class MinFunction<T> implements Function<Stream<T>, T> {
      private final Comparator<? super T> comparator;

      private MinFunction(Comparator<? super T> comparator) {
         this.comparator = comparator;
      }

      @Override
      public T apply(Stream<T> stream) {
         return stream.min(comparator).orElse(null);
      }

      public static final class MinFunctionExternalizer implements Externalizer<MinFunction> {
         @Override
         public void writeObject(ObjectOutput output, MinFunction object) throws IOException {
            output.writeObject(object.comparator);
         }

         @Override
         public MinFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new MinFunction((Comparator) input.readObject());
         }
      }
   }

   @SerializeWith(value = MinDoubleFunction.MinDoubleFunctionExternalizer.class)
   private static final class MinDoubleFunction<T> implements Function<DoubleStream, Double> {
      private static final MinDoubleFunction OPERATION = new MinDoubleFunction();

      private MinDoubleFunction() { }

      public static MinDoubleFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Double apply(DoubleStream stream) {
         OptionalDouble op = stream.min();
         if (op.isPresent()) {
            return op.getAsDouble();
         } else {
            return null;
         }
      }

      public static final class MinDoubleFunctionExternalizer implements Externalizer<MinDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, MinDoubleFunction object) throws IOException {
         }

         @Override
         public MinDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return MinDoubleFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = MinIntFunction.MinIntFunctionExternalizer.class)
   private static final class MinIntFunction<T> implements Function<IntStream, Integer> {
      private static final MinIntFunction OPERATION = new MinIntFunction();

      private MinIntFunction() { }

      public static MinIntFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Integer apply(IntStream stream) {
         OptionalInt op = stream.min();
         if (op.isPresent()) {
            return op.getAsInt();
         } else {
            return null;
         }
      }

      public static final class MinIntFunctionExternalizer implements Externalizer<MinIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, MinIntFunction object) throws IOException {
         }

         @Override
         public MinIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return MinIntFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = MinLongFunction.MinLongFunctionExternalizer.class)
   private static final class MinLongFunction<T> implements Function<LongStream, Long> {
      private static final MinLongFunction OPERATION = new MinLongFunction();

      private MinLongFunction() { }

      public static MinLongFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Long apply(LongStream stream) {
         OptionalLong op = stream.min();
         if (op.isPresent()) {
            return op.getAsLong();
         } else {
            return null;
         }
      }

      public static final class MinLongFunctionExternalizer implements Externalizer<MinLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, MinLongFunction object) throws IOException {
         }

         @Override
         public MinLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return MinLongFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = ReduceFunction.ReduceFunctionExternalizer.class)
   private static final class ReduceFunction<T> implements Function<Stream<T>, T> {
      private final BinaryOperator<T> accumulator;

      private ReduceFunction(BinaryOperator<T> accumulator) {
         this.accumulator = accumulator;
      }

      @Override
      public T apply(Stream<T> stream) {
         return stream.reduce(accumulator).orElse(null);
      }

      public static final class ReduceFunctionExternalizer implements Externalizer<ReduceFunction> {

         @Override
         public void writeObject(ObjectOutput output, ReduceFunction object) throws IOException {
            output.writeObject(object.accumulator);
         }

         @Override
         public ReduceFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ReduceFunction((BinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = ReduceDoubleFunction.ReduceDoubleFunctionExternalizer.class)
   private static final class ReduceDoubleFunction<T> implements Function<DoubleStream, Double> {
      private final DoubleBinaryOperator accumulator;

      private ReduceDoubleFunction(DoubleBinaryOperator accumulator) {
         this.accumulator = accumulator;
      }

      @Override
      public Double apply(DoubleStream stream) {
         OptionalDouble optionalInt = stream.reduce(accumulator);
         if (optionalInt.isPresent()) {
            return optionalInt.getAsDouble();
         } else {
            return null;
         }
      }

      public static final class ReduceDoubleFunctionExternalizer implements Externalizer<ReduceDoubleFunction> {

         @Override
         public void writeObject(ObjectOutput output, ReduceDoubleFunction object) throws IOException {
            output.writeObject(object.accumulator);
         }

         @Override
         public ReduceDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ReduceDoubleFunction((DoubleBinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = ReduceIntFunction.ReduceIntFunctionExternalizer.class)
   private static final class ReduceIntFunction<T> implements Function<IntStream, Integer> {
      private final IntBinaryOperator accumulator;

      private ReduceIntFunction(IntBinaryOperator accumulator) {
         this.accumulator = accumulator;
      }

      @Override
      public Integer apply(IntStream stream) {
         OptionalInt optionalInt = stream.reduce(accumulator);
         if (optionalInt.isPresent()) {
            return optionalInt.getAsInt();
         } else {
            return null;
         }
      }

      public static final class ReduceIntFunctionExternalizer implements Externalizer<ReduceIntFunction> {

         @Override
         public void writeObject(ObjectOutput output, ReduceIntFunction object) throws IOException {
            output.writeObject(object.accumulator);
         }

         @Override
         public ReduceIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ReduceIntFunction((IntBinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = ReduceLongFunction.ReduceLongFunctionExternalizer.class)
   private static final class ReduceLongFunction<T> implements Function<LongStream, Long> {
      private final LongBinaryOperator accumulator;

      private ReduceLongFunction(LongBinaryOperator accumulator) {
         this.accumulator = accumulator;
      }

      @Override
      public Long apply(LongStream stream) {
         OptionalLong optionalInt = stream.reduce(accumulator);
         if (optionalInt.isPresent()) {
            return optionalInt.getAsLong();
         } else {
            return null;
         }
      }

      public static final class ReduceLongFunctionExternalizer implements Externalizer<ReduceLongFunction> {

         @Override
         public void writeObject(ObjectOutput output, ReduceLongFunction object) throws IOException {
            output.writeObject(object.accumulator);
         }

         @Override
         public ReduceLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ReduceLongFunction((LongBinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = IdentityReduceFunction.IdentityReductFunctionExternalizer.class)
   private static final class IdentityReduceFunction<T> implements Function<Stream<T>, T> {
      private final T identity;
      private final BinaryOperator<T> accumulator;

      private IdentityReduceFunction(T identity, BinaryOperator<T> accumulator) {
         this.identity = identity;
         this.accumulator = accumulator;
      }

      @Override
      public T apply(Stream<T> stream) {
         return stream.reduce(identity, accumulator);
      }

      public static final class IdentityReductFunctionExternalizer implements Externalizer<IdentityReduceFunction> {

         @Override
         public void writeObject(ObjectOutput output, IdentityReduceFunction object) throws IOException {
            output.writeObject(object.identity);
            output.writeObject(object.accumulator);
         }

         @Override
         public IdentityReduceFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdentityReduceFunction(input.readObject(), (BinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = IdentityReduceDoubleFunction.IdentityReductFunctionExternalizer.class)
   private static final class IdentityReduceDoubleFunction<T> implements Function<DoubleStream, Double> {
      private final double identity;
      private final DoubleBinaryOperator accumulator;

      private IdentityReduceDoubleFunction(double identity, DoubleBinaryOperator accumulator) {
         this.identity = identity;
         this.accumulator = accumulator;
      }

      @Override
      public Double apply(DoubleStream stream) {
         return stream.reduce(identity, accumulator);
      }

      public static final class IdentityReductFunctionExternalizer implements Externalizer<IdentityReduceDoubleFunction> {

         @Override
         public void writeObject(ObjectOutput output, IdentityReduceDoubleFunction object) throws IOException {
            output.writeDouble(object.identity);
            output.writeObject(object.accumulator);
         }

         @Override
         public IdentityReduceDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdentityReduceDoubleFunction(input.readDouble(), (DoubleBinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = IdentityReduceIntFunction.IdentityReductFunctionExternalizer.class)
   private static final class IdentityReduceIntFunction<T> implements Function<IntStream, Integer> {
      private final int identity;
      private final IntBinaryOperator accumulator;

      private IdentityReduceIntFunction(int identity, IntBinaryOperator accumulator) {
         this.identity = identity;
         this.accumulator = accumulator;
      }

      @Override
      public Integer apply(IntStream stream) {
         return stream.reduce(identity, accumulator);
      }

      public static final class IdentityReductFunctionExternalizer implements Externalizer<IdentityReduceIntFunction> {

         @Override
         public void writeObject(ObjectOutput output, IdentityReduceIntFunction object) throws IOException {
            output.writeInt(object.identity);
            output.writeObject(object.accumulator);
         }

         @Override
         public IdentityReduceIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdentityReduceIntFunction(input.readInt(), (IntBinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = IdentityReduceLongFunction.IdentityReductFunctionExternalizer.class)
   private static final class IdentityReduceLongFunction<T> implements Function<LongStream, Long> {
      private final long identity;
      private final LongBinaryOperator accumulator;

      private IdentityReduceLongFunction(long identity, LongBinaryOperator accumulator) {
         this.identity = identity;
         this.accumulator = accumulator;
      }

      @Override
      public Long apply(LongStream stream) {
         return stream.reduce(identity, accumulator);
      }

      public static final class IdentityReductFunctionExternalizer implements Externalizer<IdentityReduceLongFunction> {

         @Override
         public void writeObject(ObjectOutput output, IdentityReduceLongFunction object) throws IOException {
            output.writeLong(object.identity);
            output.writeObject(object.accumulator);
         }

         @Override
         public IdentityReduceLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdentityReduceLongFunction(input.readLong(), (LongBinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = IdentityReduceCombinerFunction.IdentityReductFunctionExternalizer.class)
   private static final class IdentityReduceCombinerFunction<T, R> implements Function<Stream<T>, R> {
      private final R identity;
      private final BiFunction<R, ? super T, R> accumulator;
      private final BinaryOperator<R> combiner;

      private IdentityReduceCombinerFunction(R identity, BiFunction<R, ? super T, R> accumulator,
              BinaryOperator<R> combiner) {
         this.identity = identity;
         this.accumulator = accumulator;
         this.combiner = combiner;
      }

      @Override
      public R apply(Stream<T> stream) {
         return stream.reduce(identity, accumulator, combiner);
      }

      public static final class IdentityReductFunctionExternalizer implements Externalizer<IdentityReduceCombinerFunction> {

         @Override
         public void writeObject(ObjectOutput output, IdentityReduceCombinerFunction object) throws IOException {
            output.writeObject(object.identity);
            output.writeObject(object.accumulator);
            output.writeObject(object.combiner);
         }

         @Override
         public IdentityReduceCombinerFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdentityReduceCombinerFunction(input.readObject(), (BiFunction) input.readObject(),
                    (BinaryOperator) input.readObject());
         }
      }
   }

   @SerializeWith(value = SumDoubleFunction.SumDoubleFunctionExternalizer.class)
   private static final class SumDoubleFunction implements Function<DoubleStream, Double> {
      private static final SumDoubleFunction OPERATION = new SumDoubleFunction();

      private SumDoubleFunction() { }

      public static SumDoubleFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Double apply(DoubleStream stream) {
         return stream.sum();
      }

      public static final class SumDoubleFunctionExternalizer implements Externalizer<SumDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, SumDoubleFunction object) throws IOException {
         }

         @Override
         public SumDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return SumDoubleFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = SumIntFunction.SumIntFunctionExternalizer.class)
   private static final class SumIntFunction implements Function<IntStream, Integer> {
      private static final SumIntFunction OPERATION = new SumIntFunction();

      private SumIntFunction() { }

      public static SumIntFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Integer apply(IntStream stream) {
         return stream.sum();
      }

      public static final class SumIntFunctionExternalizer implements Externalizer<SumIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, SumIntFunction object) throws IOException {
         }

         @Override
         public SumIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return SumIntFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = SumLongFunction.SumLongFunctionExternalizer.class)
   private static final class SumLongFunction implements Function<LongStream, Long> {
      private static final SumLongFunction OPERATION = new SumLongFunction();

      private SumLongFunction() { }

      public static SumLongFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Long apply(LongStream stream) {
         return stream.sum();
      }

      public static final class SumLongFunctionExternalizer implements Externalizer<SumLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, SumLongFunction object) throws IOException {
         }

         @Override
         public SumLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return SumLongFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = ToArrayFunction.ToArrayFunctionExternalizer.class)
   private static final class ToArrayFunction<T> implements Function<Stream<T>, Object[]> {
      private static final ToArrayFunction OPERATION = new ToArrayFunction();

      private ToArrayFunction() { }

      public static ToArrayFunction getInstance() {
         return OPERATION;
      }

      @Override
      public Object[] apply(Stream<T> stream) {
         return stream.toArray();
      }

      public static final class ToArrayFunctionExternalizer implements Externalizer<ToArrayFunction> {
         @Override
         public void writeObject(ObjectOutput output, ToArrayFunction object) throws IOException {
         }

         @Override
         public ToArrayFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return ToArrayFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = ToArrayDoubleFunction.ToArrayDoubleFunctionExternalizer.class)
   private static final class ToArrayDoubleFunction implements Function<DoubleStream, double[]> {
      private static final ToArrayDoubleFunction OPERATION = new ToArrayDoubleFunction();

      private ToArrayDoubleFunction() { }

      public static ToArrayDoubleFunction getInstance() {
         return OPERATION;
      }

      @Override
      public double[] apply(DoubleStream stream) {
         return stream.toArray();
      }

      public static final class ToArrayDoubleFunctionExternalizer implements Externalizer<ToArrayDoubleFunction> {
         @Override
         public void writeObject(ObjectOutput output, ToArrayDoubleFunction object) throws IOException {
         }

         @Override
         public ToArrayDoubleFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return ToArrayDoubleFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = ToArrayIntFunction.ToArrayIntFunctionExternalizer.class)
   private static final class ToArrayIntFunction implements Function<IntStream, int[]> {
      private static final ToArrayIntFunction OPERATION = new ToArrayIntFunction();

      private ToArrayIntFunction() { }

      public static ToArrayIntFunction getInstance() {
         return OPERATION;
      }

      @Override
      public int[] apply(IntStream stream) {
         return stream.toArray();
      }

      public static final class ToArrayIntFunctionExternalizer implements Externalizer<ToArrayIntFunction> {
         @Override
         public void writeObject(ObjectOutput output, ToArrayIntFunction object) throws IOException {
         }

         @Override
         public ToArrayIntFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return ToArrayIntFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = ToArrayLongFunction.ToArrayLongFunctionExternalizer.class)
   private static final class ToArrayLongFunction implements Function<LongStream, long[]> {
      private static final ToArrayLongFunction OPERATION = new ToArrayLongFunction();

      private ToArrayLongFunction() { }

      public static ToArrayLongFunction getInstance() {
         return OPERATION;
      }

      @Override
      public long[] apply(LongStream stream) {
         return stream.toArray();
      }

      public static final class ToArrayLongFunctionExternalizer implements Externalizer<ToArrayLongFunction> {
         @Override
         public void writeObject(ObjectOutput output, ToArrayLongFunction object) throws IOException {
         }

         @Override
         public ToArrayLongFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return ToArrayLongFunction.getInstance();
         }
      }
   }

   @SerializeWith(value = ToArrayGeneratorFunction.ToArrayGeneratorFunctionExternalizer.class)
   private static final class ToArrayGeneratorFunction<T, R> implements Function<Stream<R>, T[]> {
      private final IntFunction<T[]> generator;

      private ToArrayGeneratorFunction(IntFunction<T[]> generator1) {
         this.generator = generator1;
      }

      @Override
      public T[] apply(Stream<R> stream) {
         return stream.toArray(generator);
      }

      public static final class ToArrayGeneratorFunctionExternalizer implements Externalizer<ToArrayGeneratorFunction> {
         @Override
         public void writeObject(ObjectOutput output, ToArrayGeneratorFunction object) throws IOException {
            output.writeObject(object.generator);
         }

         @Override
         public ToArrayGeneratorFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new ToArrayGeneratorFunction((IntFunction) input.readObject());
         }
      }
   }
}
