package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.impl.intops.primitive.i.*;
import org.infinispan.stream.CacheAware;

import java.util.*;
import java.util.function.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * IntStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalIntCacheStream extends AbstractLocalCacheStream<Integer, IntStream> implements IntStream {
   /**
    * @param streamSupplier
    * @param parallel
    * @param registry
    */
   public LocalIntCacheStream(StreamSupplier streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   LocalIntCacheStream(AbstractLocalCacheStream<?, ?> original) {
      super(original);
   }

   @Override
   public IntStream filter(IntPredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterIntOperation<>(predicate));
      return this;
   }

   @Override
   public IntStream map(IntUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapIntOperation(mapper));
      return this;
   }

   @Override
   public <U> Stream<U> mapToObj(IntFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjIntOperation<>(mapper));
      return new LocalCacheStream<U>(this);
   }

   @Override
   public LongStream mapToLong(IntToLongFunction mapper) {
      intermediateOperations.add(new MapToLongIntOperation(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public DoubleStream mapToDouble(IntToDoubleFunction mapper) {
      intermediateOperations.add(new MapToDoubleIntOperation(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public IntStream flatMap(IntFunction<? extends IntStream> mapper) {
      intermediateOperations.add(new FlatMapIntOperation(mapper));
      return this;
   }

   @Override
   public IntStream distinct() {
      intermediateOperations.add(DistinctIntOperation.getInstance());
      return this;
   }

   @Override
   public IntStream sorted() {
      intermediateOperations.add(SortedIntOperation.getInstance());
      return this;
   }

   @Override
   public IntStream peek(IntConsumer action) {
      intermediateOperations.add(new PeekIntOperation(action));
      return this;
   }

   @Override
   public IntStream limit(long maxSize) {
      intermediateOperations.add(new LimitIntOperation(maxSize));
      return this;
   }

   @Override
   public IntStream skip(long n) {
      intermediateOperations.add(new SkipIntOperation(n));
      return this;
   }

   @Override
   public void forEach(IntConsumer action) {
      injectCache(action);
      createStream().forEach(action);
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
   public LongStream asLongStream() {
      return null;
   }

   @Override
   public DoubleStream asDoubleStream() {
      return null;
   }

   @Override
   public Stream<Integer> boxed() {
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

}
