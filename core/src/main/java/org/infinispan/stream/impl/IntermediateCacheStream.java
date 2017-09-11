package org.infinispan.stream.impl;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.BaseCacheStream;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableBinaryOperator;
import org.infinispan.util.function.SerializableComparator;
import org.infinispan.util.function.SerializableConsumer;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.function.SerializableIntFunction;
import org.infinispan.util.function.SerializablePredicate;
import org.infinispan.util.function.SerializableSupplier;
import org.infinispan.util.function.SerializableToDoubleFunction;
import org.infinispan.util.function.SerializableToIntFunction;
import org.infinispan.util.function.SerializableToLongFunction;

/**
 * An intermediate cache stream used when an intermediate operation that requires both a remote and local portion
 */
public class IntermediateCacheStream<R> implements CacheStream<R> {
   private BaseCacheStream remoteStream;
   private final IntermediateType type;
   private LocalCacheStream<R> localStream;

   private final IntermediateCacheStreamSupplier supplier;

   public IntermediateCacheStream(DistributedCacheStream<R> remoteStream) {
      this.remoteStream = remoteStream;
      this.type = IntermediateType.REF;
      this.supplier = new IntermediateCacheStreamSupplier(type, remoteStream);
      this.localStream = new LocalCacheStream<>(supplier, remoteStream.parallel,
              remoteStream.registry);
   }

   public IntermediateCacheStream(BaseCacheStream remoteStream, IntermediateType type,
           LocalCacheStream<R> localStream, IntermediateCacheStreamSupplier supplier) {
      this.remoteStream = remoteStream;
      this.type = type;
      this.localStream = localStream;
      this.supplier = supplier;
   }

   @Override
   public CacheStream<R> sequentialDistribution() {
      remoteStream = remoteStream.sequentialDistribution();
      return this;
   }

   @Override
   public CacheStream<R> parallelDistribution() {
      remoteStream = remoteStream.parallelDistribution();
      return this;
   }

   @Override
   public CacheStream<R> filterKeySegments(Set<Integer> segments) {
      remoteStream = remoteStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public CacheStream<R> filterKeys(Set<?> keys) {
      remoteStream = remoteStream.filterKeys(keys);
      return this;
   }

   @Override
   public CacheStream<R> distributedBatchSize(int batchSize) {
      remoteStream = remoteStream.distributedBatchSize(batchSize);
      return this;
   }

   @Override
   public CacheStream<R> segmentCompletionListener(SegmentCompletionListener listener) {
      throw new UnsupportedOperationException("Segment completion listener is only supported when no intermediate " +
              "operation is provided (sorted, distinct, limit, skip)");
   }

   @Override
   public CacheStream<R> disableRehashAware() {
      remoteStream = remoteStream.disableRehashAware();
      return this;
   }

   @Override
   public CacheStream<R> timeout(long timeout, TimeUnit unit) {
      remoteStream = remoteStream.timeout(timeout, unit);
      return this;
   }

   @Override
   public boolean isParallel() {
      return localStream.isParallel();
   }

   @Override
   public CacheStream<R> sorted() {
      localStream = localStream.sorted();
      return this;
   }

   @Override
   public CacheStream<R> sorted(Comparator<? super R> comparator) {
      localStream = localStream.sorted(comparator);
      return this;
   }

   @Override
   public CacheStream<R> sorted(SerializableComparator<? super R> comparator) {
      return sorted((Comparator<? super R>) comparator);
   }

   @Override
   public CacheStream<R> limit(long maxSize) {
      localStream = localStream.limit(maxSize);
      return this;
   }

   @Override
   public CacheStream<R> skip(long n) {
      localStream = localStream.skip(n);
      return this;
   }

   @Override
   public CacheStream<R> peek(Consumer<? super R> action) {
      localStream = localStream.peek(action);
      return this;
   }

   @Override
   public CacheStream<R> peek(SerializableConsumer<? super R> action) {
      return peek((Consumer<? super R>) action);
   }

   @Override
   public CacheStream<R> distinct() {
      localStream = localStream.distinct();
      return this;
   }

   @Override
   public CacheStream<R> filter(Predicate<? super R> predicate) {
      localStream = localStream.filter(predicate);
      return this;
   }

   @Override
   public CacheStream<R> filter(SerializablePredicate<? super R> predicate) {
      return filter((Predicate<? super R>) predicate);
   }

   @Override
   public <R1> CacheStream<R1> map(Function<? super R, ? extends R1> mapper) {
      localStream = (LocalCacheStream<R>) localStream.map(mapper);
      return (CacheStream<R1>) this;
   }

   @Override
   public <R1> CacheStream<R1> map(SerializableFunction<? super R, ? extends R1> mapper) {
      return map((Function<? super R, ? extends R1>) mapper);
   }

   @Override
   public DoubleCacheStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      return new IntermediateDoubleCacheStream(remoteStream, type, localStream.mapToDouble(mapper), supplier);
   }

   @Override
   public DoubleCacheStream mapToDouble(SerializableToDoubleFunction<? super R> mapper) {
      return mapToDouble((ToDoubleFunction<? super R>) mapper);
   }

   @Override
   public IntCacheStream mapToInt(ToIntFunction<? super R> mapper) {
      return new IntermediateIntCacheStream(remoteStream, type, localStream.mapToInt(mapper), supplier);
   }

   @Override
   public IntCacheStream mapToInt(SerializableToIntFunction<? super R> mapper) {
      return mapToInt((ToIntFunction<? super R>) mapper);
   }

   @Override
   public LongCacheStream mapToLong(ToLongFunction<? super R> mapper) {
      return new IntermediateLongCacheStream(remoteStream, type, localStream.mapToLong(mapper), supplier);
   }

   @Override
   public LongCacheStream mapToLong(SerializableToLongFunction<? super R> mapper) {
      return mapToLong((ToLongFunction<? super R>) mapper);
   }

   @Override
   public <R1> CacheStream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      localStream = (LocalCacheStream<R>) localStream.flatMap(mapper);
      return (CacheStream<R1>) this;
   }

   @Override
   public <R1> CacheStream<R1> flatMap(SerializableFunction<? super R, ? extends Stream<? extends R1>> mapper) {
      return flatMap((Function<? super R, ? extends Stream<? extends R1>>) mapper);
   }

   @Override
   public DoubleCacheStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      return new IntermediateDoubleCacheStream(remoteStream, type, localStream.flatMapToDouble(mapper), supplier);
   }

   @Override
   public DoubleCacheStream flatMapToDouble(SerializableFunction<? super R, ? extends DoubleStream> mapper) {
      return flatMapToDouble((Function<? super R, ? extends DoubleStream>) mapper);
   }

   @Override
   public IntCacheStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      return new IntermediateIntCacheStream(remoteStream, type, localStream.flatMapToInt(mapper), supplier);
   }

   @Override
   public IntCacheStream flatMapToInt(SerializableFunction<? super R, ? extends IntStream> mapper) {
      return flatMapToInt((Function<? super R, ? extends IntStream>) mapper);
   }

   @Override
   public LongCacheStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      return new IntermediateLongCacheStream(remoteStream, type, localStream.flatMapToLong(mapper), supplier);
   }

   @Override
   public LongCacheStream flatMapToLong(SerializableFunction<? super R, ? extends LongStream> mapper) {
      return flatMapToLong((Function<? super R, ? extends LongStream>) mapper);
   }

   @Override
   public CacheStream<R> parallel() {
      remoteStream = (BaseCacheStream) remoteStream.parallel();
      localStream = (LocalCacheStream) localStream.parallel();
      return this;
   }

   @Override
   public CacheStream<R> sequential() {
      remoteStream = (BaseCacheStream) remoteStream.sequential();
      localStream = (LocalCacheStream) localStream.sequential();
      return this;
   }

   @Override
   public CacheStream<R> unordered() {
      localStream = (LocalCacheStream<R>) localStream.unordered();
      return this;
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      localStream.forEach(action);
   }

   @Override
   public void forEach(SerializableConsumer<? super R> action) {
      forEach((Consumer<? super R>) action);
   }

   @Override
   public void forEachOrdered(Consumer<? super R> action) {
      localStream.forEachOrdered(action);
   }

   @Override
   public <K, V> void forEach(BiConsumer<Cache<K, V>, ? super R> action) {
      localStream.forEach(action);
   }

   @Override
   public <K, V> void forEach(SerializableBiConsumer<Cache<K, V>, ? super R> action) {
      forEach((BiConsumer<Cache<K, V>, ? super R>) action);
   }

   @Override
   public R reduce(R identity, BinaryOperator<R> accumulator) {
      return localStream.reduce(identity, accumulator);
   }

   @Override
   public R reduce(R identity, SerializableBinaryOperator<R> accumulator) {
      return reduce(identity, (BinaryOperator<R>) accumulator);
   }

   @Override
   public Optional<R> reduce(BinaryOperator<R> accumulator) {
      return localStream.reduce(accumulator);
   }

   @Override
   public Optional<R> reduce(SerializableBinaryOperator<R> accumulator) {
      return reduce((BinaryOperator<R>) accumulator);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return localStream.reduce(identity, accumulator, combiner);
   }

   @Override
   public <U> U reduce(U identity, SerializableBiFunction<U, ? super R, U> accumulator, SerializableBinaryOperator<U> combiner) {
      return reduce(identity, (BiFunction<U, ? super R, U>) accumulator, combiner);
   }

   @Override
   public <R1, A> R1 collect(Collector<? super R, A, R1> collector) {
      return localStream.collect(collector);
   }

   @Override
   public <R1, A> R1 collect(SerializableSupplier<Collector<? super R, A, R1>> supplier) {
      return localStream.collect(supplier);
   }

   @Override
   public <R1> R1 collect(Supplier<R1> supplier, BiConsumer<R1, ? super R> accumulator, BiConsumer<R1, R1> combiner) {
      return localStream.collect(supplier, accumulator, combiner);
   }

   @Override
   public <R1> R1 collect(SerializableSupplier<R1> supplier, SerializableBiConsumer<R1, ? super R> accumulator,
           SerializableBiConsumer<R1, R1> combiner) {
      return collect((Supplier<R1>) supplier, accumulator, combiner);
   }

   @Override
   public Optional<R> max(Comparator<? super R> comparator) {
      return localStream.max(comparator);
   }

   @Override
   public Optional<R> max(SerializableComparator<? super R> comparator) {
      return max((Comparator<? super R>) comparator);
   }

   @Override
   public Optional<R> min(Comparator<? super R> comparator) {
      return localStream.min(comparator);
   }

   @Override
   public Optional<R> min(SerializableComparator<? super R> comparator) {
      return min((Comparator<? super R>) comparator);
   }

   @Override
   public long count() {
      return localStream.count();
   }

   @Override
   public boolean anyMatch(Predicate<? super R> predicate) {
      return localStream.anyMatch(predicate);
   }

   @Override
   public boolean anyMatch(SerializablePredicate<? super R> predicate) {
      return anyMatch((Predicate<? super R>) predicate);
   }

   @Override
   public boolean allMatch(Predicate<? super R> predicate) {
      return localStream.allMatch(predicate);
   }

   @Override
   public boolean allMatch(SerializablePredicate<? super R> predicate) {
      return allMatch((Predicate<? super R>) predicate);
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return localStream.noneMatch(predicate);
   }

   @Override
   public boolean noneMatch(SerializablePredicate<? super R> predicate) {
      return noneMatch((Predicate<? super R>) predicate);
   }

   @Override
   public Optional<R> findFirst() {
      return localStream.findFirst();
   }

   @Override
   public Optional<R> findAny() {
      return localStream.findAny();
   }

   @Override
   public Iterator<R> iterator() {
      return localStream.iterator();
   }

   @Override
   public Spliterator<R> spliterator() {
      return localStream.spliterator();
   }

   @Override
   public Object[] toArray() {
      return new Object[0];
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      return localStream.toArray(generator);
   }

   @Override
   public <A> A[] toArray(SerializableIntFunction<A[]> generator) {
      return toArray((IntFunction<A[]>) generator);
   }

   @Override
   public CacheStream<R> onClose(Runnable closeHandler) {
      remoteStream = (BaseCacheStream) remoteStream.onClose(closeHandler);
      return this;
   }

   @Override
   public void close() {
      localStream.close();
      remoteStream.close();
   }
}
