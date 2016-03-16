package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
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
import org.infinispan.util.function.SerializableIntBinaryOperator;
import org.infinispan.util.function.SerializableIntConsumer;
import org.infinispan.util.function.SerializableIntPredicate;
import org.infinispan.util.function.SerializableIntToDoubleFunction;
import org.infinispan.util.function.SerializableIntToLongFunction;
import org.infinispan.util.function.SerializableIntUnaryOperator;
import org.infinispan.util.function.SerializableObjIntConsumer;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableIntFunction;
import org.infinispan.util.function.SerializableSupplier;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
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

/**
 * IntStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalIntCacheStream extends AbstractLocalCacheStream<Integer, IntStream, IntCacheStream> implements IntCacheStream {
   LocalIntCacheStream(AbstractLocalCacheStream<?, ?, ?> original) {
      super(original);
   }

   @Override
   public IntCacheStream filter(IntPredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterIntOperation<>(predicate));
      return this;
   }

   @Override
   public IntCacheStream filter(SerializableIntPredicate predicate) {
      return filter((IntPredicate) predicate);
   }

   @Override
   public IntCacheStream map(IntUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapIntOperation(mapper));
      return this;
   }

   @Override
   public IntCacheStream map(SerializableIntUnaryOperator mapper) {
      return map((IntUnaryOperator) mapper);
   }

   @Override
   public <U> CacheStream<U> mapToObj(IntFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjIntOperation<>(mapper));
      return new LocalCacheStream<U>(this);
   }

   @Override
   public <U> CacheStream<U> mapToObj(SerializableIntFunction<? extends U> mapper) {
      return mapToObj((IntFunction<? extends U>) mapper);
   }

   @Override
   public LongCacheStream mapToLong(IntToLongFunction mapper) {
      intermediateOperations.add(new MapToLongIntOperation(mapper));
      return new LocalLongCacheStream(this);
   }

   @Override
   public LongCacheStream mapToLong(SerializableIntToLongFunction mapper) {
      return mapToLong((IntToLongFunction) mapper);
   }

   @Override
   public DoubleCacheStream mapToDouble(IntToDoubleFunction mapper) {
      intermediateOperations.add(new MapToDoubleIntOperation(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public DoubleCacheStream mapToDouble(SerializableIntToDoubleFunction mapper) {
      return mapToDouble((IntToDoubleFunction) mapper);
   }

   @Override
   public IntCacheStream flatMap(IntFunction<? extends IntStream> mapper) {
      intermediateOperations.add(new FlatMapIntOperation(mapper));
      return this;
   }

   @Override
   public IntCacheStream flatMap(SerializableIntFunction<? extends IntStream> mapper) {
      return flatMap((IntFunction<? extends IntStream>) mapper);
   }

   @Override
   public IntCacheStream distinct() {
      intermediateOperations.add(DistinctIntOperation.getInstance());
      return this;
   }

   @Override
   public IntCacheStream sorted() {
      intermediateOperations.add(SortedIntOperation.getInstance());
      return this;
   }

   @Override
   public IntCacheStream peek(IntConsumer action) {
      intermediateOperations.add(new PeekIntOperation(action));
      return this;
   }

   @Override
   public IntCacheStream peek(SerializableIntConsumer action) {
      return peek((IntConsumer) action);
   }

   @Override
   public IntCacheStream limit(long maxSize) {
      intermediateOperations.add(new LimitIntOperation(maxSize));
      return this;
   }

   @Override
   public IntCacheStream skip(long n) {
      intermediateOperations.add(new SkipIntOperation(n));
      return this;
   }

   @Override
   public void forEach(IntConsumer action) {
      injectCache(action);
      createStream().forEach(action);
   }

   @Override
   public void forEach(SerializableIntConsumer action) {
      forEach((IntConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjIntConsumer<Cache<K, V>> action) {
      Cache<K, V> cache = registry.getComponent(Cache.class);
      createStream().forEach(i -> action.accept(cache, i));
   }

   @Override
   public <K, V> void forEach(SerializableObjIntConsumer<Cache<K, V>> action) {
      forEach((ObjIntConsumer<Cache<K, V>>) action);
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
   public int reduce(int identity, SerializableIntBinaryOperator op) {
      return reduce(identity, (IntBinaryOperator) op);
   }

   @Override
   public OptionalInt reduce(IntBinaryOperator op) {
      return createStream().reduce(op);
   }

   @Override
   public OptionalInt reduce(SerializableIntBinaryOperator op) {
      return reduce((IntBinaryOperator) op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return createStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public <R> R collect(SerializableSupplier<R> supplier, SerializableObjIntConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
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
   public boolean anyMatch(SerializableIntPredicate predicate) {
      return anyMatch((IntPredicate) predicate);
   }

   @Override
   public boolean allMatch(IntPredicate predicate) {
      return createStream().allMatch(predicate);
   }

   @Override
   public boolean allMatch(SerializableIntPredicate predicate) {
      return allMatch((IntPredicate) predicate);
   }

   @Override
   public boolean noneMatch(IntPredicate predicate) {
      return createStream().noneMatch(predicate);
   }

   @Override
   public boolean noneMatch(SerializableIntPredicate predicate) {
      return noneMatch((IntPredicate) predicate);
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
      return null;
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      return null;
   }

   @Override
   public CacheStream<Integer> boxed() {
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
