package org.infinispan.stream.impl.local;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.primitive.i.BoxedIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.DistinctIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.FilterIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.FlatMapIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.LimitIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.MapIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.MapToDoubleIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.MapToLongIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.MapToObjIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.PeekIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.SkipIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.SortedIntOperation;

/**
 * IntStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalIntCacheStream extends AbstractLocalCacheStream<Integer, IntStream, IntCacheStream> implements IntCacheStream {
   public LocalIntCacheStream(StreamSupplier<Integer, IntStream> streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   LocalIntCacheStream(AbstractLocalCacheStream<?, ?, ?> original) {
      super(original);
   }

   @Override
   public LocalIntCacheStream filter(IntPredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterIntOperation<>(predicate));
      return this;
   }

   @Override
   public LocalIntCacheStream map(IntUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapIntOperation(mapper));
      return this;
   }

   @Override
   public <U> LocalCacheStream<U> mapToObj(IntFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjIntOperation<>(mapper));
      return new LocalCacheStream<U>(this);
   }

   @Override
   public LocalLongCacheStream mapToLong(IntToLongFunction mapper) {
      intermediateOperations.add(new MapToLongIntOperation(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public LocalDoubleCacheStream mapToDouble(IntToDoubleFunction mapper) {
      intermediateOperations.add(new MapToDoubleIntOperation(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public LocalIntCacheStream flatMap(IntFunction<? extends IntStream> mapper) {
      intermediateOperations.add(new FlatMapIntOperation(mapper));
      return this;
   }

   @Override
   public LocalIntCacheStream distinct() {
      intermediateOperations.add(DistinctIntOperation.getInstance());
      return this;
   }

   @Override
   public LocalIntCacheStream sorted() {
      intermediateOperations.add(SortedIntOperation.getInstance());
      return this;
   }

   @Override
   public LocalIntCacheStream peek(IntConsumer action) {
      intermediateOperations.add(new PeekIntOperation(action));
      return this;
   }

   @Override
   public LocalIntCacheStream limit(long maxSize) {
      intermediateOperations.add(new LimitIntOperation(maxSize));
      return this;
   }

   @Override
   public LocalIntCacheStream skip(long n) {
      intermediateOperations.add(new SkipIntOperation(n));
      return this;
   }

   @Override
   public void forEach(IntConsumer action) {
      injectCache(action);
      createStream().forEach(action);
   }

   @Override
   public <K, V> void forEach(ObjIntConsumer<Cache<K, V>> action) {
      Cache<K, V> cache = registry.getComponent(Cache.class);
      createStream().forEach(i -> action.accept(cache, i));
   }

   @Override
   public void forEachOrdered(IntConsumer action) {
      injectCache(action);
      createStream().forEachOrdered(action);
   }

   /**
    * Method to inject a cache into a consumer.  Note we only support this for the consumer at this
    * time.
    * @param cacheAware the instance that may be a {@link CacheAware}
    */
   private void injectCache(IntConsumer cacheAware) {
      if (cacheAware instanceof CacheAware) {
         ((CacheAware) cacheAware).injectCache(registry.getComponent(Cache.class));
      }
   }

   @Override
   public int[] toArray() {
      return createStream().toArray();
   }

   @Override
   public int reduce(int identity, IntBinaryOperator op) {
      return createStream().reduce(identity, op);
   }

   @Override
   public OptionalInt reduce(IntBinaryOperator op) {
      return createStream().reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return createStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public int sum() {
      return createStream().sum();
   }

   @Override
   public OptionalInt min() {
      return createStream().min();
   }

   @Override
   public OptionalInt max() {
      return createStream().max();
   }

   @Override
   public long count() {
      return createStream().count();
   }

   @Override
   public OptionalDouble average() {
      return createStream().average();
   }

   @Override
   public IntSummaryStatistics summaryStatistics() {
      return createStream().summaryStatistics();
   }

   @Override
   public boolean anyMatch(IntPredicate predicate) {
      return createStream().anyMatch(predicate);
   }

   @Override
   public boolean allMatch(IntPredicate predicate) {
      return createStream().allMatch(predicate);
   }

   @Override
   public boolean noneMatch(IntPredicate predicate) {
      return createStream().noneMatch(predicate);
   }

   @Override
   public OptionalInt findFirst() {
      return createStream().findFirst();
   }

   @Override
   public OptionalInt findAny() {
      return createStream().findAny();
   }

   @Override
   public LongCacheStream asLongStream() {
      return mapToLong(i -> (long) i);
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      return mapToDouble(i -> (double) i);
   }

   @Override
   public LocalCacheStream<Integer> boxed() {
      intermediateOperations.add(BoxedIntOperation.getInstance());
      return new LocalCacheStream<>(this);
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return createStream().iterator();
   }

   @Override
   public Spliterator.OfInt spliterator() {
      return createStream().spliterator();
   }

   @Override
   public LocalIntCacheStream sequentialDistribution() {
      return this;
   }

   @Override
   public LocalIntCacheStream parallelDistribution() {
      return this;
   }

   @Override
   public LocalIntCacheStream filterKeySegments(Set<Integer> segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public LocalIntCacheStream filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public LocalIntCacheStream distributedBatchSize(int batchSize) {
      // TODO: Does this change cache loader?
      return this;
   }

   @Override
   public LocalIntCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      // All segments are completed when the getStream() is completed so we don't track them
      return this;
   }

   @Override
   public LocalIntCacheStream disableRehashAware() {
      // Local long stream doesn't matter for rehash
      return this;
   }

   @Override
   public LocalIntCacheStream timeout(long timeout, TimeUnit unit) {
      // Timeout does nothing for a local long cache stream
      return this;
   }
}
