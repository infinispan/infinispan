package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.stream.CacheAware;
import org.infinispan.stream.impl.intops.primitive.d.*;

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * DoubleStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalDoubleCacheStream extends AbstractLocalCacheStream<Double, DoubleStream> implements DoubleStream {
   /**
    * @param streamSupplier
    * @param parallel
    * @param registry
    */
   public LocalDoubleCacheStream(StreamSupplier streamSupplier, boolean parallel, ComponentRegistry registry) {
      super(streamSupplier, parallel, registry);
   }

   LocalDoubleCacheStream(AbstractLocalCacheStream<?, ?> original) {
      super(original);
   }

   @Override
   public DoubleStream filter(DoublePredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterDoubleOperation(predicate));
      return this;
   }

   @Override
   public DoubleStream map(DoubleUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapDoubleOperation(mapper));
      return this;
   }

   @Override
   public <U> Stream<U> mapToObj(DoubleFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjDoubleOperation<>(mapper));
      return new LocalCacheStream<U>(this);
   }

   @Override
   public IntStream mapToInt(DoubleToIntFunction mapper) {
      intermediateOperations.add(new MapToIntDoubleOperation(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public LongStream mapToLong(DoubleToLongFunction mapper) {
      intermediateOperations.add(new MapToLongDoubleOperation(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public DoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
      intermediateOperations.add(new FlatMapDoubleOperation(mapper));
      return this;
   }

   @Override
   public DoubleStream distinct() {
      intermediateOperations.add(DistinctDoubleOperation.getInstance());
      return this;
   }

   @Override
   public DoubleStream sorted() {
      intermediateOperations.add(SortedDoubleOperation.getInstance());
      return this;
   }

   @Override
   public DoubleStream peek(DoubleConsumer action) {
      intermediateOperations.add(new PeekDoubleOperation(action));
      return this;
   }

   @Override
   public DoubleStream limit(long maxSize) {
      intermediateOperations.add(new LimitDoubleOperation(maxSize));
      return this;
   }

   @Override
   public DoubleStream skip(long n) {
      intermediateOperations.add(new SkipDoubleOperation(n));
      return this;
   }

   @Override
   public void forEach(DoubleConsumer action) {
      injectCache(action);
      createStream().forEach(action);
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
   public Stream<Double> boxed() {
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

}
