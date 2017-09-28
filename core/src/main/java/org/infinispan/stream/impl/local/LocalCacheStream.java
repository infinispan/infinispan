package org.infinispan.stream.impl.local;

import java.util.Comparator;
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

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.object.DistinctOperation;
import org.infinispan.stream.impl.intops.object.FilterOperation;
import org.infinispan.stream.impl.intops.object.FlatMapOperation;
import org.infinispan.stream.impl.intops.object.FlatMapToDoubleOperation;
import org.infinispan.stream.impl.intops.object.FlatMapToIntOperation;
import org.infinispan.stream.impl.intops.object.FlatMapToLongOperation;
import org.infinispan.stream.impl.intops.object.LimitOperation;
import org.infinispan.stream.impl.intops.object.MapOperation;
import org.infinispan.stream.impl.intops.object.MapToDoubleOperation;
import org.infinispan.stream.impl.intops.object.MapToIntOperation;
import org.infinispan.stream.impl.intops.object.MapToLongOperation;
import org.infinispan.stream.impl.intops.object.PeekOperation;
import org.infinispan.stream.impl.intops.object.SkipOperation;
import org.infinispan.stream.impl.intops.object.SortedComparatorOperation;
import org.infinispan.stream.impl.intops.object.SortedOperation;
import org.infinispan.util.function.RemovableFunction;

/**
 * CacheStream that is to be used locally.  This allows for full functionality of a regular stream but also has options
 * to filter by keys and other functionality.
 * @param <R> type of the stream
 */
public class LocalCacheStream<R> extends AbstractLocalCacheStream<R, Stream<R>, CacheStream<R>> implements CacheStream<R> {

   public LocalCacheStream(StreamSupplier<R, Stream<R>> streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   public LocalCacheStream(AbstractLocalCacheStream<?, ?, ?> other) {
      super(other);
   }

   @Override
   public LocalCacheStream<R> sequentialDistribution() {
      return this;
   }

   @Override
   public LocalCacheStream<R> parallelDistribution() {
      return this;
   }

   @Override
   public LocalCacheStream<R> filterKeySegments(Set<Integer> segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public LocalCacheStream<R> filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public LocalCacheStream<R> distributedBatchSize(int batchSize) {
      // TODO: Does this change cache loader?
      return this;
   }

   @Override
   public LocalCacheStream<R> segmentCompletionListener(SegmentCompletionListener listener) {
      // All segments are completed when the getStream() is completed so we don't track them
      return this;
   }

   @Override
   public LocalCacheStream<R> disableRehashAware() {
      // Local stream doesn't matter for rehash
      return this;
   }

   @Override
   public LocalCacheStream<R> filter(Predicate<? super R> predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterOperation<>(predicate));
      return this;
   }

   @Override
   public <R1> LocalCacheStream<R1> map(Function<? super R, ? extends R1> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapOperation<>(mapper));
      return (LocalCacheStream<R1>) this;
   }

   @Override
   public LocalIntCacheStream mapToInt(ToIntFunction<? super R> mapper) {
      intermediateOperations.add(new MapToIntOperation<>(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public LocalLongCacheStream mapToLong(ToLongFunction<? super R> mapper) {
      intermediateOperations.add(new MapToLongOperation<>(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public LocalDoubleCacheStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      intermediateOperations.add(new MapToDoubleOperation<>(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public <R1> LocalCacheStream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      intermediateOperations.add(new FlatMapOperation<>(mapper));
      return (LocalCacheStream<R1>) this;
   }

   @Override
   public LocalIntCacheStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      intermediateOperations.add(new FlatMapToIntOperation<>(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public LocalLongCacheStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      intermediateOperations.add(new FlatMapToLongOperation<>(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public LocalDoubleCacheStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      intermediateOperations.add(new FlatMapToDoubleOperation<>(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public LocalCacheStream<R> distinct() {
      intermediateOperations.add(DistinctOperation.getInstance());
      return this;
   }

   @Override
   public LocalCacheStream<R> sorted() {
      intermediateOperations.add(SortedOperation.getInstance());
      return this;
   }

   @Override
   public LocalCacheStream<R> sorted(Comparator<? super R> comparator) {
      intermediateOperations.add(new SortedComparatorOperation<>(comparator));
      return this;
   }

   @Override
   public LocalCacheStream<R> peek(Consumer<? super R> action) {
      intermediateOperations.add(new PeekOperation<>(action));
      return this;
   }

   @Override
   public LocalCacheStream<R> limit(long maxSize) {
      intermediateOperations.add(new LimitOperation<>(maxSize));
      return this;
   }

   @Override
   public LocalCacheStream<R> skip(long n) {
      intermediateOperations.add(new SkipOperation<>(n));
      return this;
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      injectCache(action);
      createStream().forEach(action);
   }

   @Override
   public <K, V> void forEach(BiConsumer<Cache<K, V>, ? super R> action) {
      Cache<K, V> cache = registry.getComponent(Cache.class);
      registry.wireDependencies(action);
      createStream().forEach(e -> action.accept(cache, e));
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
      int size = intermediateOperations.size();
      if (size == 0) {
         // If no intermediate operations we can support remove
         return streamSupplier.removableIterator(Closeables.iterator(createStream()));
      }
      else if (size == 1) {
         IntermediateOperation intOp = intermediateOperations.peek();
         if (intOp instanceof MapOperation) {
            MapOperation map = (MapOperation) intOp;
            if (map.getFunction() instanceof RemovableFunction) {
               // If function was removable means we can just use remove as is
               return streamSupplier.removableIterator(Closeables.iterator(createStream()));
            }
         }
      }
      return Closeables.iterator(createStream());
   }

   @Override
   public Spliterator<R> spliterator() {
      return createStream().spliterator();
   }

   @Override
   public LocalCacheStream<R> timeout(long timeout, TimeUnit unit) {
      // Timeout does nothing for a local cache stream
      return this;
   }
}
