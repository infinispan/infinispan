package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.object.*;
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

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * CacheStream that is to be used locally.  This allows for full functionality of a regular stream but also has options
 * to filter by keys and other functionality.
 * @param <R> type of the stream
 */
public class LocalCacheStream<R> extends AbstractLocalCacheStream<R, Stream<R>, CacheStream<R>> implements CacheStream<R> {

   public LocalCacheStream(StreamSupplier<R> streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   public LocalCacheStream(AbstractLocalCacheStream<?, ?, ?> other) {
      super(other);
   }

   @Override
   public CacheStream<R> sequentialDistribution() {
      return this;
   }

   @Override
   public CacheStream<R> parallelDistribution() {
      return this;
   }

   @Override
   public CacheStream<R> filterKeySegments(Set<Integer> segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public CacheStream<R> filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public CacheStream<R> distributedBatchSize(int batchSize) {
      // TODO: Does this change cache loader?
      return this;
   }

   @Override
   public CacheStream<R> segmentCompletionListener(SegmentCompletionListener listener) {
      // All segments are completed when the getStream() is completed so we don't track them
      return this;
   }

   @Override
   public CacheStream<R> disableRehashAware() {
      // Local stream doesn't matter for rehash
      return this;
   }

   @Override
   public CacheStream<R> filter(Predicate<? super R> predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterOperation<>(predicate));
      return this;
   }

   @Override
   public CacheStream<R> filter(SerializablePredicate<? super R> predicate) {
      return filter((Predicate<? super R>) predicate);
   }

   @Override
   public <R1> CacheStream<R1> map(Function<? super R, ? extends R1> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapOperation<>(mapper));
      return (CacheStream<R1>) this;
   }

   @Override
   public <R1> CacheStream<R1> map(SerializableFunction<? super R, ? extends R1> mapper) {
      return map((Function<? super R, ? extends R1>) mapper);
   }

   @Override
   public IntCacheStream mapToInt(ToIntFunction<? super R> mapper) {
      intermediateOperations.add(new MapToIntOperation<>(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public IntCacheStream mapToInt(SerializableToIntFunction<? super R> mapper) {
      return mapToInt((ToIntFunction<? super R>) mapper);
   }

   @Override
   public LongCacheStream mapToLong(ToLongFunction<? super R> mapper) {
      intermediateOperations.add(new MapToLongOperation<>(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public LongCacheStream mapToLong(SerializableToLongFunction<? super R> mapper) {
      return mapToLong((ToLongFunction<? super R>) mapper);
   }

   @Override
   public DoubleCacheStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      intermediateOperations.add(new MapToDoubleOperation<>(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public DoubleCacheStream mapToDouble(SerializableToDoubleFunction<? super R> mapper) {
      return mapToDouble((ToDoubleFunction<? super R>) mapper);
   }

   @Override
   public <R1> CacheStream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      intermediateOperations.add(new FlatMapOperation<>(mapper));
      return (CacheStream<R1>) this;
   }

   @Override
   public <R1> CacheStream<R1> flatMap(SerializableFunction<? super R, ? extends Stream<? extends R1>> mapper) {
      return flatMap((Function<? super R, ? extends Stream<? extends R1>>) mapper);
   }

   @Override
   public IntCacheStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      intermediateOperations.add(new FlatMapToIntOperation<>(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public IntCacheStream flatMapToInt(SerializableFunction<? super R, ? extends IntStream> mapper) {
      return flatMapToInt((Function<? super R, ? extends IntStream>) mapper);
   }

   @Override
   public LongCacheStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      intermediateOperations.add(new FlatMapToLongOperation<>(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public LongCacheStream flatMapToLong(SerializableFunction<? super R, ? extends LongStream> mapper) {
      return flatMapToLong((Function<? super R, ? extends LongStream>) mapper);
   }

   @Override
   public DoubleCacheStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      intermediateOperations.add(new FlatMapToDoubleOperation<>(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public DoubleCacheStream flatMapToDouble(SerializableFunction<? super R, ? extends DoubleStream> mapper) {
      return flatMapToDouble((Function<? super R, ? extends DoubleStream>) mapper);
   }

   @Override
   public CacheStream<R> distinct() {
      intermediateOperations.add(DistinctOperation.getInstance());
      return this;
   }

   @Override
   public CacheStream<R> sorted() {
      intermediateOperations.add(SortedOperation.getInstance());
      return this;
   }

   @Override
   public CacheStream<R> sorted(Comparator<? super R> comparator) {
      intermediateOperations.add(new SortedComparatorOperation<>(comparator));
      return this;
   }

   @Override
   public CacheStream<R> sorted(SerializableComparator<? super R> comparator) {
      return sorted((Comparator<? super R>) comparator);
   }

   @Override
   public CacheStream<R> peek(Consumer<? super R> action) {
      intermediateOperations.add(new PeekOperation<>(action));
      return this;
   }

   @Override
   public CacheStream<R> peek(SerializableConsumer<? super R> action) {
      return peek((Consumer<? super R>) action);
   }

   @Override
   public CacheStream<R> limit(long maxSize) {
      intermediateOperations.add(new LimitOperation<>(maxSize));
      return this;
   }

   @Override
   public CacheStream<R> skip(long n) {
      intermediateOperations.add(new SkipOperation<>(n));
      return this;
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      injectCache(action);
      createStream().forEach(action);
   }

   @Override
   public void forEach(SerializableConsumer<? super R> action) {
      forEach((Consumer<? super R>) action);
   }

   @Override
   public <K, V> void forEach(BiConsumer<Cache<K, V>, ? super R> action) {
      Cache<K, V> cache = registry.getComponent(Cache.class);
      createStream().forEach(e -> action.accept(cache, e));
   }

   @Override
   public <K, V> void forEach(SerializableBiConsumer<Cache<K, V>, ? super R> action) {
      forEach((BiConsumer<Cache<K, V>, ? super R>) action);
   }

   @Override
   public void forEachOrdered(Consumer<? super R> action) {
      injectCache(action);
      createStream().forEachOrdered(action);
   }

   /**
    * Method to inject a cache into a consumer.  Note we only support this for the consumer at this
    * time.
    * @param cacheAware the instance that may be a {@link CacheAware}
    */
   private void injectCache(Consumer<? super R> cacheAware) {
      if (cacheAware instanceof CacheAware) {
         ((CacheAware) cacheAware).injectCache(registry.getComponent(Cache.class));
      }
   }

   @Override
   public Object[] toArray() {
      return createStream().toArray();
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      return createStream().toArray(generator);
   }

   @Override
   public <A> A[] toArray(SerializableIntFunction<A[]> generator) {
      return toArray((IntFunction<A[]>) generator);
   }

   @Override
   public R reduce(R identity, BinaryOperator<R> accumulator) {
      return createStream().reduce(identity, accumulator);
   }

   @Override
   public R reduce(R identity, SerializableBinaryOperator<R> accumulator) {
      return reduce(identity, (BinaryOperator<R>) accumulator);
   }

   @Override
   public Optional<R> reduce(BinaryOperator<R> accumulator) {
      return createStream().reduce(accumulator);
   }

   @Override
   public Optional<R> reduce(SerializableBinaryOperator<R> accumulator) {
      return reduce((BinaryOperator<R>) accumulator);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return createStream().reduce(identity, accumulator, combiner);
   }

   @Override
   public <U> U reduce(U identity, SerializableBiFunction<U, ? super R, U> accumulator, SerializableBinaryOperator<U> combiner) {
      return reduce(identity, (BiFunction<U, ? super R, U>) accumulator, combiner);
   }

   @Override
   public <R1> R1 collect(Supplier<R1> supplier, BiConsumer<R1, ? super R> accumulator, BiConsumer<R1, R1> combiner) {
      return createStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public <R1> R1 collect(SerializableSupplier<R1> supplier, SerializableBiConsumer<R1, ? super R> accumulator, SerializableBiConsumer<R1, R1> combiner) {
      return collect((Supplier<R1>) supplier, accumulator, combiner);
   }

   @Override
   public <R1, A> R1 collect(Collector<? super R, A, R1> collector) {
      return createStream().collect(collector);
   }

   @Override
   public Optional<R> min(Comparator<? super R> comparator) {
      return createStream().min(comparator);
   }

   @Override
   public Optional<R> min(SerializableComparator<? super R> comparator) {
      return min((Comparator<? super R>) comparator);
   }

   @Override
   public Optional<R> max(Comparator<? super R> comparator) {
      return createStream().max(comparator);
   }

   @Override
   public Optional<R> max(SerializableComparator<? super R> comparator) {
      return max((Comparator<? super R>) comparator);
   }

   @Override
   public long count() {
      return createStream().count();
   }

   @Override
   public boolean anyMatch(Predicate<? super R> predicate) {
      return createStream().anyMatch(predicate);
   }

   @Override
   public boolean anyMatch(SerializablePredicate<? super R> predicate) {
      return anyMatch((Predicate<? super R>) predicate);
   }

   @Override
   public boolean allMatch(Predicate<? super R> predicate) {
      return createStream().allMatch(predicate);
   }

   @Override
   public boolean allMatch(SerializablePredicate<? super R> predicate) {
      return allMatch((Predicate<? super R>) predicate);
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return createStream().noneMatch(predicate);
   }

   @Override
   public boolean noneMatch(SerializablePredicate<? super R> predicate) {
      return noneMatch((Predicate<? super R>) predicate);
   }

   @Override
   public Optional<R> findFirst() {
      return createStream().findFirst();
   }

   @Override
   public Optional<R> findAny() {
      return createStream().findAny();
   }

   @Override
   public CloseableIterator<R> iterator() {
      // If the stream is null we can assume no intermediate operations and thus we can support remove
      if (intermediateOperations.isEmpty()) {
         return streamSupplier.removableIterator(Closeables.iterator(createStream()));
      } else {
         return Closeables.iterator(createStream());
      }
   }

   @Override
   public Spliterator<R> spliterator() {
      return createStream().spliterator();
   }

   @Override
   public CacheStream<R> timeout(long timeout, TimeUnit unit) {
      // Timeout does nothing for a local cache stream
      return this;
   }
}
