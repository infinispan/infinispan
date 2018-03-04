package org.infinispan.stream.impl.local;

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;

import org.infinispan.Cache;
import org.infinispan.DoubleCacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.primitive.d.BoxedDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.DistinctDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.FilterDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.FlatMapDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.LimitDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.MapDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.MapToIntDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.MapToLongDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.MapToObjDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.PeekDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.SkipDoubleOperation;
import org.infinispan.stream.impl.intops.primitive.d.SortedDoubleOperation;

/**
 * DoubleStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalDoubleCacheStream extends AbstractLocalCacheStream<Double, DoubleStream, DoubleCacheStream> implements DoubleCacheStream {
   public LocalDoubleCacheStream(StreamSupplier<Double, DoubleStream> streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   LocalDoubleCacheStream(AbstractLocalCacheStream<?, ?, ?> original) {
      super(original);
   }

   @Override
   public LocalDoubleCacheStream filter(DoublePredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterDoubleOperation(predicate));
      return this;
   }

   @Override
   public LocalDoubleCacheStream map(DoubleUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapDoubleOperation(mapper));
      return this;
   }

   @Override
   public <U> LocalCacheStream<U> mapToObj(DoubleFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjDoubleOperation<>(mapper));
      return new LocalCacheStream<>(this);
   }

   @Override
   public LocalIntCacheStream mapToInt(DoubleToIntFunction mapper) {
      intermediateOperations.add(new MapToIntDoubleOperation(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public LocalLongCacheStream mapToLong(DoubleToLongFunction mapper) {
      intermediateOperations.add(new MapToLongDoubleOperation(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public LocalDoubleCacheStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
      intermediateOperations.add(new FlatMapDoubleOperation(mapper));
      return this;
   }

   @Override
   public LocalDoubleCacheStream distinct() {
      intermediateOperations.add(DistinctDoubleOperation.getInstance());
      return this;
   }

   @Override
   public LocalDoubleCacheStream sorted() {
      intermediateOperations.add(SortedDoubleOperation.getInstance());
      return this;
   }

   @Override
   public LocalDoubleCacheStream peek(DoubleConsumer action) {
      intermediateOperations.add(new PeekDoubleOperation(action));
      return this;
   }

   @Override
   public LocalDoubleCacheStream limit(long maxSize) {
      intermediateOperations.add(new LimitDoubleOperation(maxSize));
      return this;
   }

   @Override
   public LocalDoubleCacheStream skip(long n) {
      intermediateOperations.add(new SkipDoubleOperation(n));
      return this;
   }

   @Override
   public void forEach(DoubleConsumer action) {
      injectCache(action);
      createStream().forEach(action);
   }

   @Override
   public <K, V> void forEach(ObjDoubleConsumer<Cache<K, V>> action) {
      Cache<K, V> cache = registry.getComponent(Cache.class);
      createStream().forEach(d -> action.accept(cache, d));
   }

   @Override
   public void forEachOrdered(DoubleConsumer action) {
      injectCache(action);
      createStream().forEachOrdered(action);
   }

   /**
    * Method to inject a cache into a consumer.  Note we only support this for the consumer at this
    * time.
    * @param cacheAware the instance that may be a {@link CacheAware}
    */
   private void injectCache(DoubleConsumer cacheAware) {
      if (cacheAware instanceof CacheAware) {
         ((CacheAware) cacheAware).injectCache(registry.getComponent(Cache.class));
      }
   }

   @Override
   public double[] toArray() {
      return createStream().toArray();
   }

   @Override
   public double reduce(double identity, DoubleBinaryOperator op) {
      return createStream().reduce(identity, op);
   }

   @Override
   public OptionalDouble reduce(DoubleBinaryOperator op) {
      return createStream().reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return createStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public double sum() {
      return createStream().sum();
   }

   @Override
   public OptionalDouble min() {
      return createStream().min();
   }

   @Override
   public OptionalDouble max() {
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
   public DoubleSummaryStatistics summaryStatistics() {
      return createStream().summaryStatistics();
   }

   @Override
   public boolean anyMatch(DoublePredicate predicate) {
      return createStream().anyMatch(predicate);
   }

   @Override
   public boolean allMatch(DoublePredicate predicate) {
      return createStream().allMatch(predicate);
   }

   @Override
   public boolean noneMatch(DoublePredicate predicate) {
      return createStream().noneMatch(predicate);
   }

   @Override
   public OptionalDouble findFirst() {
      return createStream().findFirst();
   }

   @Override
   public OptionalDouble findAny() {
      return createStream().findAny();
   }

   @Override
   public LocalCacheStream<Double> boxed() {
      intermediateOperations.add(BoxedDoubleOperation.getInstance());
      return new LocalCacheStream<>(this);
   }

   @Override
   public PrimitiveIterator.OfDouble iterator() {
      return createStream().iterator();
   }

   @Override
   public Spliterator.OfDouble spliterator() {
      return createStream().spliterator();
   }

   @Override
   public LocalDoubleCacheStream sequentialDistribution() {
      return this;
   }

   @Override
   public LocalDoubleCacheStream parallelDistribution() {
      return this;
   }

   @Override
   public LocalDoubleCacheStream filterKeySegments(Set<Integer> segments) {
      segmentsToFilter = SmallIntSet.from(segments);
      return this;
   }

   @Override
   public LocalDoubleCacheStream filterKeySegments(IntSet segments) {
      segmentsToFilter = SmallIntSet.from(segments);
      return this;
   }

   @Override
   public LocalDoubleCacheStream filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public LocalDoubleCacheStream distributedBatchSize(int batchSize) {
      // TODO: Does this change cache loader?
      return this;
   }

   @Override
   public LocalDoubleCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      // All segments are completed when the getStream() is completed so we don't track them
      return this;
   }

   @Override
   public LocalDoubleCacheStream disableRehashAware() {
      // Local long stream doesn't matter for rehash
      return this;
   }

   @Override
   public LocalDoubleCacheStream timeout(long timeout, TimeUnit unit) {
      // Timeout does nothing for a local long cache stream
      return this;
   }
}
