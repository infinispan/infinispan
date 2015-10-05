package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.object.*;

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
public class LocalCacheStream<R> extends AbstractLocalCacheStream<R, Stream<R>> implements CacheStream<R> {

   public LocalCacheStream(StreamSupplier<R> streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   public LocalCacheStream(AbstractLocalCacheStream<?, ?> other) {
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
   public Stream<R> filter(Predicate<? super R> predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterOperation<>(predicate));
      return this;
   }

   @Override
   public <R1> Stream<R1> map(Function<? super R, ? extends R1> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapOperation<>(mapper));
      return (Stream<R1>) this;
   }

   @Override
   public IntStream mapToInt(ToIntFunction<? super R> mapper) {
      intermediateOperations.add(new MapToIntOperation<>(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public LongStream mapToLong(ToLongFunction<? super R> mapper) {
      intermediateOperations.add(new MapToLongOperation<>(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public DoubleStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      intermediateOperations.add(new MapToDoubleOperation<>(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public <R1> Stream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      intermediateOperations.add(new FlatMapOperation<>(mapper));
      return (Stream<R1>) this;
   }

   @Override
   public IntStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      intermediateOperations.add(new FlatMapToIntOperation<>(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public LongStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      intermediateOperations.add(new FlatMapToLongOperation<>(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public DoubleStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      intermediateOperations.add(new FlatMapToDoubleOperation<>(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public Stream<R> distinct() {
      intermediateOperations.add(DistinctOperation.getInstance());
      return this;
   }

   @Override
   public Stream<R> sorted() {
      intermediateOperations.add(SortedOperation.getInstance());
      return this;
   }

   @Override
   public Stream<R> sorted(Comparator<? super R> comparator) {
      intermediateOperations.add(new SortedComparatorOperation<>(comparator));
      return this;
   }

   @Override
   public Stream<R> peek(Consumer<? super R> action) {
      intermediateOperations.add(new PeekOperation<>(action));
      return this;
   }

   @Override
   public Stream<R> limit(long maxSize) {
      intermediateOperations.add(new LimitOperation<>(maxSize));
      return this;
   }

   @Override
   public Stream<R> skip(long n) {
      intermediateOperations.add(new SkipOperation<>(n));
      return this;
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      injectCache(action);
      createStream().forEach(action);
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
   public R reduce(R identity, BinaryOperator<R> accumulator) {
      return createStream().reduce(identity, accumulator);
   }

   @Override
   public Optional<R> reduce(BinaryOperator<R> accumulator) {
      return createStream().reduce(accumulator);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return createStream().reduce(identity, accumulator, combiner);
   }

   @Override
   public <R1> R1 collect(Supplier<R1> supplier, BiConsumer<R1, ? super R> accumulator, BiConsumer<R1, R1> combiner) {
      return createStream().collect(supplier, accumulator, combiner);
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
   public Optional<R> max(Comparator<? super R> comparator) {
      return createStream().max(comparator);
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
   public boolean allMatch(Predicate<? super R> predicate) {
      return createStream().allMatch(predicate);
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return createStream().noneMatch(predicate);
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
