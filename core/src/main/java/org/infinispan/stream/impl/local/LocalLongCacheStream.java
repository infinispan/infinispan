package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
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
import org.infinispan.util.function.SerializableLongBinaryOperator;
import org.infinispan.util.function.SerializableLongConsumer;
import org.infinispan.util.function.SerializableLongFunction;
import org.infinispan.util.function.SerializableLongPredicate;
import org.infinispan.util.function.SerializableLongToDoubleFunction;
import org.infinispan.util.function.SerializableLongToIntFunction;
import org.infinispan.util.function.SerializableLongUnaryOperator;
import org.infinispan.util.function.SerializableObjLongConsumer;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableSupplier;

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
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

/**
 * LongStream that wraps a given stream to allow for additional functionality such as injection of values into
 * various operations
 */
public class LocalLongCacheStream extends AbstractLocalCacheStream<Long, LongStream, LongCacheStream> implements LongCacheStream {
   LocalLongCacheStream(AbstractLocalCacheStream<?, ?, ?> original) {
      super(original);
   }

   @Override
   public LongCacheStream filter(LongPredicate predicate) {
      registry.wireDependencies(predicate);
      intermediateOperations.add(new FilterLongOperation<>(predicate));
      return this;
   }

   @Override
   public LongCacheStream filter(SerializableLongPredicate predicate) {
      return filter((LongPredicate) predicate);
   }

   @Override
   public LongCacheStream map(LongUnaryOperator mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapLongOperation(mapper));
      return this;
   }

   @Override
   public LongCacheStream map(SerializableLongUnaryOperator mapper) {
      return map((LongUnaryOperator) mapper);
   }

   @Override
   public <U> CacheStream<U> mapToObj(LongFunction<? extends U> mapper) {
      registry.wireDependencies(mapper);
      intermediateOperations.add(new MapToObjLongOperation<>(mapper));
      return new LocalCacheStream<U>(this);
   }

   @Override
   public <U> CacheStream<U> mapToObj(SerializableLongFunction<? extends U> mapper) {
      return mapToObj((LongFunction<? extends U>) mapper);
   }

   @Override
   public IntCacheStream mapToInt(LongToIntFunction mapper) {
      intermediateOperations.add(new MapToIntLongOperation(mapper));
      return new LocalIntCacheStream(this);
   }

   @Override
   public IntCacheStream mapToInt(SerializableLongToIntFunction mapper) {
      return mapToInt((LongToIntFunction) mapper);
   }

   @Override
   public DoubleCacheStream mapToDouble(LongToDoubleFunction mapper) {
      intermediateOperations.add(new MapToDoubleLongOperation(mapper));
      return new LocalDoubleCacheStream(this);
   }

   @Override
   public DoubleCacheStream mapToDouble(SerializableLongToDoubleFunction mapper) {
      return mapToDouble((LongToDoubleFunction) mapper);
   }

   @Override
   public LongCacheStream flatMap(LongFunction<? extends LongStream> mapper) {
      intermediateOperations.add(new FlatMapLongOperation(mapper));
      return this;
   }

   @Override
   public LongCacheStream flatMap(SerializableLongFunction<? extends LongStream> mapper) {
      return flatMap((LongFunction<? extends LongStream>) mapper);
   }

   @Override
   public LongCacheStream distinct() {
      intermediateOperations.add(DistinctLongOperation.getInstance());
      return this;
   }

   @Override
   public LongCacheStream sorted() {
      intermediateOperations.add(SortedLongOperation.getInstance());
      return this;
   }

   @Override
   public LongCacheStream peek(LongConsumer action) {
      intermediateOperations.add(new PeekLongOperation(action));
      return this;
   }

   @Override
   public LongCacheStream peek(SerializableLongConsumer action) {
      return peek((LongConsumer) action);
   }

   @Override
   public LongCacheStream limit(long maxSize) {
      intermediateOperations.add(new LimitLongOperation(maxSize));
      return this;
   }

   @Override
   public LongCacheStream skip(long n) {
      intermediateOperations.add(new SkipLongOperation(n));
      return this;
   }

   @Override
   public void forEach(LongConsumer action) {
      injectCache(action);
      createStream().forEach(action);
   }

   @Override
   public void forEach(SerializableLongConsumer action) {
      forEach((LongConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjLongConsumer<Cache<K, V>> action) {
      Cache<K, V> cache = registry.getComponent(Cache.class);
      createStream().forEach(l -> action.accept(cache, l));
   }

   @Override
   public <K, V> void forEach(SerializableObjLongConsumer<Cache<K, V>> action) {
      forEach((ObjLongConsumer<Cache<K, V>>) action);
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
   public long reduce(long identity, SerializableLongBinaryOperator op) {
      return reduce(identity, (LongBinaryOperator) op);
   }

   @Override
   public OptionalLong reduce(LongBinaryOperator op) {
      return createStream().reduce(op);
   }

   @Override
   public OptionalLong reduce(SerializableLongBinaryOperator op) {
      return reduce((LongBinaryOperator) op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return createStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public <R> R collect(SerializableSupplier<R> supplier, SerializableObjLongConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
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
   public boolean anyMatch(SerializableLongPredicate predicate) {
      return anyMatch((LongPredicate) predicate);
   }

   @Override
   public boolean allMatch(LongPredicate predicate) {
      return createStream().allMatch(predicate);
   }

   @Override
   public boolean allMatch(SerializableLongPredicate predicate) {
      return allMatch((LongPredicate) predicate);
   }

   @Override
   public boolean noneMatch(LongPredicate predicate) {
      return createStream().noneMatch(predicate);
   }

   @Override
   public boolean noneMatch(SerializableLongPredicate predicate) {
      return noneMatch((LongPredicate) predicate);
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
      return null;
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

}
