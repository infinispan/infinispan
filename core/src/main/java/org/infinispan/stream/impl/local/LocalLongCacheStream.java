package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.primitive.l.*;

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * LongStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalLongCacheStream extends AbstractLocalCacheStream<Long, LongStream> implements LongStream {
   /**
    * @param streamSupplier
    * @param parallel
    * @param registry
    */
   public LocalLongCacheStream(StreamSupplier streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   LocalLongCacheStream(AbstractLocalCacheStream<?, ?> original) {
      super(original);
   }

   @Override
   public LongStream filter(LongPredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterLongOperation<>(predicate));
      return this;
   }

   @Override
   public LongStream map(LongUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapLongOperation(mapper));
      return this;
   }

   @Override
   public <U> Stream<U> mapToObj(LongFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjLongOperation<>(mapper));
      return new LocalCacheStream<U>(this);
   }

   @Override
   public IntStream mapToInt(LongToIntFunction mapper) {
      intermediateOperations.add(new MapToIntLongOperation(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public DoubleStream mapToDouble(LongToDoubleFunction mapper) {
      intermediateOperations.add(new MapToDoubleLongOperation(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public LongStream flatMap(LongFunction<? extends LongStream> mapper) {
      intermediateOperations.add(new FlatMapLongOperation(mapper));
      return this;
   }

   @Override
   public LongStream distinct() {
      intermediateOperations.add(DistinctLongOperation.getInstance());
      return this;
   }

   @Override
   public LongStream sorted() {
      intermediateOperations.add(SortedLongOperation.getInstance());
      return this;
   }

   @Override
   public LongStream peek(LongConsumer action) {
      intermediateOperations.add(new PeekLongOperation(action));
      return this;
   }

   @Override
   public LongStream limit(long maxSize) {
      intermediateOperations.add(new LimitLongOperation(maxSize));
      return this;
   }

   @Override
   public LongStream skip(long n) {
      intermediateOperations.add(new SkipLongOperation(n));
      return this;
   }

   @Override
   public void forEach(LongConsumer action) {
      injectCache(action);
      createStream().forEach(action);
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
   public DoubleStream asDoubleStream() {
      return null;
   }

   @Override
   public Stream<Long> boxed() {
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

}
