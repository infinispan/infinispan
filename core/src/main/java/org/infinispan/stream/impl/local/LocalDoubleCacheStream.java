package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
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
import org.infinispan.util.function.SerializableDoubleBinaryOperator;
import org.infinispan.util.function.SerializableDoubleConsumer;
import org.infinispan.util.function.SerializableDoubleFunction;
import org.infinispan.util.function.SerializableDoublePredicate;
import org.infinispan.util.function.SerializableDoubleToIntFunction;
import org.infinispan.util.function.SerializableDoubleToLongFunction;
import org.infinispan.util.function.SerializableDoubleUnaryOperator;
import org.infinispan.util.function.SerializableObjDoubleConsumer;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableSupplier;

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
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

/**
 * DoubleStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalDoubleCacheStream extends AbstractLocalCacheStream<Double, DoubleStream, DoubleCacheStream> implements DoubleCacheStream {
   LocalDoubleCacheStream(AbstractLocalCacheStream<?, ?, ?> original) {
      super(original);
   }

   @Override
   public DoubleCacheStream filter(DoublePredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterDoubleOperation(predicate));
      return this;
   }

   @Override
   public DoubleCacheStream filter(SerializableDoublePredicate predicate) {
      return filter((DoublePredicate) predicate);
   }

   @Override
   public DoubleCacheStream map(DoubleUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapDoubleOperation(mapper));
      return this;
   }

   @Override
   public DoubleCacheStream map(SerializableDoubleUnaryOperator mapper) {
      return map((DoubleUnaryOperator) mapper);
   }

   @Override
   public <U> CacheStream<U> mapToObj(DoubleFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjDoubleOperation<>(mapper));
      return new LocalCacheStream<>(this);
   }

   @Override
   public <U> CacheStream<U> mapToObj(SerializableDoubleFunction<? extends U> mapper) {
      return mapToObj((DoubleFunction<? extends U>) mapper);
   }

   @Override
   public IntCacheStream mapToInt(DoubleToIntFunction mapper) {
      intermediateOperations.add(new MapToIntDoubleOperation(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public IntCacheStream mapToInt(SerializableDoubleToIntFunction mapper) {
      return mapToInt((DoubleToIntFunction) mapper);
   }

   @Override
   public LongCacheStream mapToLong(DoubleToLongFunction mapper) {
      intermediateOperations.add(new MapToLongDoubleOperation(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public LongCacheStream mapToLong(SerializableDoubleToLongFunction mapper) {
      return mapToLong((DoubleToLongFunction) mapper);
   }

   @Override
   public DoubleCacheStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
      intermediateOperations.add(new FlatMapDoubleOperation(mapper));
      return this;
   }

   @Override
   public DoubleCacheStream flatMap(SerializableDoubleFunction<? extends DoubleStream> mapper) {
      return flatMap((DoubleFunction<? extends DoubleStream>) mapper);
   }

   @Override
   public DoubleCacheStream distinct() {
      intermediateOperations.add(DistinctDoubleOperation.getInstance());
      return this;
   }

   @Override
   public DoubleCacheStream sorted() {
      intermediateOperations.add(SortedDoubleOperation.getInstance());
      return this;
   }

   @Override
   public DoubleCacheStream peek(DoubleConsumer action) {
      intermediateOperations.add(new PeekDoubleOperation(action));
      return this;
   }

   @Override
   public DoubleCacheStream peek(SerializableDoubleConsumer action) {
      return peek((DoubleConsumer) action);
   }

   @Override
   public DoubleCacheStream limit(long maxSize) {
      intermediateOperations.add(new LimitDoubleOperation(maxSize));
      return this;
   }

   @Override
   public DoubleCacheStream skip(long n) {
      intermediateOperations.add(new SkipDoubleOperation(n));
      return this;
   }

   @Override
   public void forEach(DoubleConsumer action) {
      injectCache(action);
      createStream().forEach(action);
   }

   @Override
   public void forEach(SerializableDoubleConsumer action) {
      forEach((DoubleConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjDoubleConsumer<Cache<K, V>> action) {
      Cache<K, V> cache = registry.getComponent(Cache.class);
      createStream().forEach(d -> action.accept(cache, d));
   }

   @Override
   public <K, V> void forEach(SerializableObjDoubleConsumer<Cache<K, V>> action) {
      forEach((ObjDoubleConsumer<Cache<K, V>>) action);
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
   public double reduce(double identity, SerializableDoubleBinaryOperator op) {
      return reduce(identity, (DoubleBinaryOperator) op);
   }

   @Override
   public OptionalDouble reduce(DoubleBinaryOperator op) {
      return createStream().reduce(op);
   }

   @Override
   public OptionalDouble reduce(SerializableDoubleBinaryOperator op) {
      return reduce((DoubleBinaryOperator) op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return createStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public <R> R collect(SerializableSupplier<R> supplier, SerializableObjDoubleConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
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
   public boolean anyMatch(SerializableDoublePredicate predicate) {
      return anyMatch((DoublePredicate) predicate);
   }

   @Override
   public boolean allMatch(DoublePredicate predicate) {
      return createStream().allMatch(predicate);
   }

   @Override
   public boolean allMatch(SerializableDoublePredicate predicate) {
      return allMatch((DoublePredicate) predicate);
   }

   @Override
   public boolean noneMatch(DoublePredicate predicate) {
      return createStream().noneMatch(predicate);
   }

   @Override
   public boolean noneMatch(SerializableDoublePredicate predicate) {
      return noneMatch((DoublePredicate) predicate);
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
   public CacheStream<Double> boxed() {
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
