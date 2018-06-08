package org.infinispan.stream.impl.local;

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.primitive.l.BoxedLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.DistinctLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.FilterLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.FlatMapLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.LimitLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.MapLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.MapToDoubleLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.MapToIntLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.MapToObjLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.PeekLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.SkipLongOperation;
import org.infinispan.stream.impl.intops.primitive.l.SortedLongOperation;

/**
 * LongStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalLongCacheStream extends AbstractLocalCacheStream<Long, LongStream, LongCacheStream> implements LongCacheStream {

   public LocalLongCacheStream(StreamSupplier<Long, LongStream> streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   LocalLongCacheStream(AbstractLocalCacheStream<?, ?, ?> original) {
      super(original);
   }

   @Override
   public LocalLongCacheStream filter(LongPredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterLongOperation<>(predicate));
      return this;
   }

   @Override
   public LocalLongCacheStream map(LongUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapLongOperation(mapper));
      return this;
   }

   @Override
   public <U> LocalCacheStream<U> mapToObj(LongFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjLongOperation<>(mapper));
      return new LocalCacheStream<U>(this);
   }

   @Override
   public LocalIntCacheStream mapToInt(LongToIntFunction mapper) {
      intermediateOperations.add(new MapToIntLongOperation(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public LocalDoubleCacheStream mapToDouble(LongToDoubleFunction mapper) {
      intermediateOperations.add(new MapToDoubleLongOperation(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public LocalLongCacheStream flatMap(LongFunction<? extends LongStream> mapper) {
      intermediateOperations.add(new FlatMapLongOperation(mapper));
      return this;
   }

   @Override
   public LocalLongCacheStream distinct() {
      intermediateOperations.add(DistinctLongOperation.getInstance());
      return this;
   }

   @Override
   public LocalLongCacheStream sorted() {
      intermediateOperations.add(SortedLongOperation.getInstance());
      return this;
   }

   @Override
   public LocalLongCacheStream peek(LongConsumer action) {
      intermediateOperations.add(new PeekLongOperation(action));
      return this;
   }

   @Override
   public LocalLongCacheStream limit(long maxSize) {
      intermediateOperations.add(new LimitLongOperation(maxSize));
      return this;
   }

   @Override
   public LocalLongCacheStream skip(long n) {
      intermediateOperations.add(new SkipLongOperation(n));
      return this;
   }

   @Override
   public void forEach(LongConsumer action) {
      injectCache(action);
      createStream().forEach(action);
   }

   @Override
   public <K, V> void forEach(ObjLongConsumer<Cache<K, V>> action) {
      Cache<K, V> cache = registry.getComponent(Cache.class);
      createStream().forEach(l -> action.accept(cache, l));
   }

   @Override
   public void forEachOrdered(LongConsumer action) {
      injectCache(action);
      createStream().forEachOrdered(action);
   }

   /**
    * Method to inject a cache into a consumer.  Note we only support this for the consumer at this
    * time.
    * @param cacheAware the instance that may be a {@link CacheAware}
    */
   private void injectCache(LongConsumer cacheAware) {
      if (cacheAware instanceof CacheAware) {
         ((CacheAware) cacheAware).injectCache(registry.getComponent(Cache.class));
      }
   }

   @Override
   public long[] toArray() {
      return createStream().toArray();
   }

   @Override
   public long reduce(long identity, LongBinaryOperator op) {
      return createStream().reduce(identity, op);
   }

   @Override
   public OptionalLong reduce(LongBinaryOperator op) {
      return createStream().reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return createStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public long sum() {
      return createStream().sum();
   }

   @Override
   public OptionalLong min() {
      return createStream().min();
   }

   @Override
   public OptionalLong max() {
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
   public LongSummaryStatistics summaryStatistics() {
      return createStream().summaryStatistics();
   }

   @Override
   public boolean anyMatch(LongPredicate predicate) {
      return createStream().anyMatch(predicate);
   }

   @Override
   public boolean allMatch(LongPredicate predicate) {
      return createStream().allMatch(predicate);
   }

   @Override
   public boolean noneMatch(LongPredicate predicate) {
      return createStream().noneMatch(predicate);
   }

   @Override
   public OptionalLong findFirst() {
      return createStream().findFirst();
   }

   @Override
   public OptionalLong findAny() {
      return createStream().findAny();
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      return mapToDouble(l -> (double) l);
   }

   @Override
   public CacheStream<Long> boxed() {
      intermediateOperations.add(BoxedLongOperation.getInstance());
      return new LocalCacheStream<>(this);
   }

   @Override
   public PrimitiveIterator.OfLong iterator() {
      return createStream().iterator();
   }

   @Override
   public Spliterator.OfLong spliterator() {
      return createStream().spliterator();
   }

   @Override
   public LocalLongCacheStream sequentialDistribution() {
      return this;
   }

   @Override
   public LocalLongCacheStream parallelDistribution() {
      return this;
   }

   @Override
   public LocalLongCacheStream filterKeySegments(Set<Integer> segments) {
      return filterKeySegments(IntSets.from(segments));
   }

   @Override
   public LocalLongCacheStream filterKeySegments(IntSet segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public LocalLongCacheStream filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public LocalLongCacheStream distributedBatchSize(int batchSize) {
      // TODO: Does this change cache loader?
      return this;
   }

   @Override
   public LocalLongCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      // All segments are completed when the getStream() is completed so we don't track them
      return this;
   }

   @Override
   public LocalLongCacheStream disableRehashAware() {
      // Local long stream doesn't matter for rehash
      return this;
   }

   @Override
   public LocalLongCacheStream timeout(long timeout, TimeUnit unit) {
      // Timeout does nothing for a local long cache stream
      return this;
   }
}
