package org.infinispan.stream.impl;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.primitive.l.AsDoubleLongOperation;
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
import org.infinispan.stream.impl.intops.primitive.l.SortedLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapObjLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachObjLongOperation;
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Implementation of {@link LongStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToInt(ToIntFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 */
public class DistributedLongCacheStream extends AbstractCacheStream<Long, LongStream, LongCacheStream>
        implements LongCacheStream {
   /**
    * This constructor is to be used only when a user calls a map or flat map method changing to an IntStream
    * from a CacheStream, Stream, DoubleStream, IntStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedLongCacheStream(AbstractCacheStream other) {
      super(other);
   }

   @Override
   protected LongCacheStream unwrap() {
      return this;
   }

   @Override
   public LongCacheStream filter(LongPredicate predicate) {
      return addIntermediateOperation(new FilterLongOperation<>(predicate));
   }

   @Override
   public LongCacheStream filter(SerializableLongPredicate predicate) {
      return filter((LongPredicate) predicate);
   }

   @Override
   public LongCacheStream map(LongUnaryOperator mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperation(new MapLongOperation(mapper));
   }

   @Override
   public LongCacheStream map(SerializableLongUnaryOperator mapper) {
      return map((LongUnaryOperator) mapper);
   }

   @Override
   public <U> CacheStream<U> mapToObj(LongFunction<? extends U> mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToObjLongOperation<>(mapper));
      return cacheStream();
   }

   @Override
   public <U> CacheStream<U> mapToObj(SerializableLongFunction<? extends U> mapper) {
      return mapToObj((LongFunction<? extends U>) mapper);
   }

   @Override
   public IntCacheStream mapToInt(LongToIntFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToIntLongOperation(mapper));
      return intCacheStream();
   }

   @Override
   public IntCacheStream mapToInt(SerializableLongToIntFunction mapper) {
      return mapToInt((LongToIntFunction) mapper);
   }

   @Override
   public DoubleCacheStream mapToDouble(LongToDoubleFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToDoubleLongOperation(mapper));
      return doubleCacheStream();
   }

   @Override
   public DoubleCacheStream mapToDouble(SerializableLongToDoubleFunction mapper) {
      return mapToDouble((LongToDoubleFunction) mapper);
   }

   @Override
   public LongCacheStream flatMap(LongFunction<? extends LongStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperation(new FlatMapLongOperation(mapper));
   }

   @Override
   public LongCacheStream flatMap(SerializableLongFunction<? extends LongStream> mapper) {
      return flatMap((LongFunction<? extends LongStream>) mapper);
   }

   @Override
   public LongCacheStream distinct() {
      DistinctLongOperation op = DistinctLongOperation.getInstance();
      markDistinct(op, IntermediateType.LONG);
      return addIntermediateOperation(op);
   }

   @Override
   public LongCacheStream sorted() {
      markSorted(IntermediateType.LONG);
      return addIntermediateOperation(SortedLongOperation.getInstance());
   }

   @Override
   public LongCacheStream peek(LongConsumer action) {
      return addIntermediateOperation(new PeekLongOperation(action));
   }

   @Override
   public LongCacheStream peek(SerializableLongConsumer action) {
      return peek((LongConsumer) action);
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      addIntermediateOperationMap(AsDoubleLongOperation.getInstance());
      return doubleCacheStream();
   }

   @Override
   public CacheStream<Long> boxed() {
      addIntermediateOperationMap(BoxedLongOperation.getInstance());
      return cacheStream();
   }

   @Override
   public LongCacheStream limit(long maxSize) {
      LimitLongOperation op = new LimitLongOperation(maxSize);
      markDistinct(op, IntermediateType.LONG);
      return addIntermediateOperation(op);
   }

   @Override
   public LongCacheStream skip(long n) {
      LimitLongOperation op = new LimitLongOperation(n);
      markSkip(IntermediateType.LONG);
      return addIntermediateOperation(op);
   }

   // Reset are terminal operators

   @Override
   public void forEach(LongConsumer action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashKeyTrackingOperation(s -> getForEach(action, s));
      }
   }

   @Override
   public void forEach(SerializableLongConsumer action) {
      forEach((LongConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjLongConsumer<Cache<K, V>> action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashKeyTrackingOperation(s -> getForEach(action, s));
      }
   }

   @Override
   public <K, V> void forEach(SerializableObjLongConsumer<Cache<K, V>> action) {
      forEach((ObjLongConsumer<Cache<K, V>>) action);
   }

   KeyTrackingTerminalOperation<Object, Long, Object> getForEach(LongConsumer consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapLongOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachLongOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   <K, V> KeyTrackingTerminalOperation<Object, Long, Object> getForEach(ObjLongConsumer<Cache<K, V>> consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapObjLongOperation(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachObjLongOperation(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   @Override
   public void forEachOrdered(LongConsumer action) {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         performIntermediateRemoteOperation((LongStream s) -> {
            s.forEachOrdered(action);
            return null;
         });
      } else {
         forEach(action);
      }
   }

   @Override
   public long[] toArray() {
      return performOperation(TerminalFunctions.toArrayLongFunction(), false,
              (v1, v2) -> {
                 long[] array = Arrays.copyOf(v1, v1.length + v2.length);
                 System.arraycopy(v2, 0, array, v1.length, v2.length);
                 return array;
              }, null, false);
   }

   @Override
   public long reduce(long identity, LongBinaryOperator op) {
      return performOperation(TerminalFunctions.reduceFunction(identity, op), true, op::applyAsLong, null);
   }

   @Override
   public long reduce(long identity, SerializableLongBinaryOperator op) {
      return reduce(identity, (LongBinaryOperator) op);
   }

   @Override
   public OptionalLong reduce(LongBinaryOperator op) {
      Long result = performOperation(TerminalFunctions.reduceFunction(op), true,
              (i1, i2) -> {
                 if (i1 != null) {
                    if (i2 != null) {
                       return op.applyAsLong(i1, i2);
                    }
                    return i1;
                 }
                 return i2;
              }, null);
      if (result == null) {
         return OptionalLong.empty();
      } else {
         return OptionalLong.of(result);
      }
   }

   @Override
   public OptionalLong reduce(SerializableLongBinaryOperator op) {
      return reduce((LongBinaryOperator) op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return performOperation(TerminalFunctions.collectFunction(supplier, accumulator, combiner), true,
              (e1, e2) -> {
                 combiner.accept(e1, e2);
                 return e1;
              }, null);
   }

   @Override
   public <R> R collect(SerializableSupplier<R> supplier, SerializableObjLongConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
   }

   @Override
   public long sum() {
      return performOperation(TerminalFunctions.sumLongFunction(), true, (i1, i2) -> i1 + i2, null);
   }

   @Override
   public OptionalLong min() {
      Long value = performOperation(TerminalFunctions.minLongFunction(), false,
              (i1, i2) -> {
                 if (i1 != null) {
                    if (i2 != null) {
                       return i1 > i2 ? i2 : i1;
                    }
                    return i1;
                 }
                 return i2;
              }, null);
      if (value == null) {
         return OptionalLong.empty();
      } else {
         return OptionalLong.of(value);
      }
   }

   @Override
   public OptionalLong max() {
      Long value = performOperation(TerminalFunctions.maxLongFunction(), false,
              (i1, i2) -> {
                 if (i1 != null) {
                    if (i2 != null) {
                       return i1 > i2 ? i1 : i2;
                    }
                    return i1;
                 }
                 return i2;
              }, null);
      if (value == null) {
         return OptionalLong.empty();
      } else {
         return OptionalLong.of(value);
      }
   }

   @Override
   public OptionalDouble average() {
      long[] results = performOperation(TerminalFunctions.averageLongFunction(), true,
              (a1, a2) -> {
                 a1[0] += a2[0];
                 a1[1] += a2[1];
                 return a1;
              }, null);
      if (results[1] > 0) {
         return OptionalDouble.of((double) results[0] / results[1]);
      } else {
         return OptionalDouble.empty();
      }
   }

   @Override
   public LongSummaryStatistics summaryStatistics() {
      return performOperation(TerminalFunctions.summaryStatisticsLongFunction(), true, (ls1, ls2) -> {
         ls1.combine(ls2);
         return ls1;
      }, null);
   }

   @Override
   public boolean anyMatch(LongPredicate predicate) {
      return performOperation(TerminalFunctions.anyMatchFunction(predicate), false, Boolean::logicalOr, b -> b);
   }

   @Override
   public boolean anyMatch(SerializableLongPredicate predicate) {
      return anyMatch((LongPredicate) predicate);
   }

   @Override
   public boolean allMatch(LongPredicate predicate) {
      return performOperation(TerminalFunctions.allMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean allMatch(SerializableLongPredicate predicate) {
      return allMatch((LongPredicate) predicate);
   }

   @Override
   public boolean noneMatch(LongPredicate predicate) {
      return performOperation(TerminalFunctions.noneMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean noneMatch(SerializableLongPredicate predicate) {
      return noneMatch((LongPredicate) predicate);
   }

   @Override
   public OptionalLong findFirst() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         return performIntermediateRemoteOperation((LongStream s) -> s.findFirst());
      } else {
         return findAny();
      }
   }

   @Override
   public OptionalLong findAny() {
      Long result = performOperation(TerminalFunctions.findAnyLongFunction(), false,
              (i1, i2) -> {
                 if (i1 != null) {
                    return i1;
                 } else {
                    return i2;
                 }
              }, null);
      if (result != null) {
         return OptionalLong.of(result);
      } else {
         return OptionalLong.empty();
      }
   }

   @Override
   public PrimitiveIterator.OfLong iterator() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         LongStream stream = performIntermediateRemoteOperation(Function.identity());
         return stream.iterator();
      } else {
         return remoteIterator();
      }
   }

   PrimitiveIterator.OfLong remoteIterator() {
      // TODO: need to add in way to not box these later
      // Since this is a remote iterator we have to add it to the remote intermediate operations queue
      intermediateOperations.add(BoxedLongOperation.getInstance());
      DistributedCacheStream<Long> stream = new DistributedCacheStream<>(this);
      Iterator<Long> iterator = stream.remoteIterator();
      return new LongIteratorToPrimitiveLong(iterator);
   }

   static class LongIteratorToPrimitiveLong implements PrimitiveIterator.OfLong {
      private final Iterator<Long> iterator;

      LongIteratorToPrimitiveLong(Iterator<Long> iterator) {
         this.iterator = iterator;
      }

      @Override
      public long nextLong() {
         return iterator.next();
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }
   }

   @Override
   public Spliterator.OfLong spliterator() {
      return Spliterators.spliteratorUnknownSize(iterator(), 0);
   }

   @Override
   public long count() {
      return performOperation(TerminalFunctions.countLongFunction(), true, (i1, i2) -> i1 + i2, null);
   }

   protected <R> DistributedCacheStream<R> cacheStream() {
      return new DistributedCacheStream<>(this);
   }

   protected DistributedDoubleCacheStream doubleCacheStream() {
      return new DistributedDoubleCacheStream(this);
   }

   protected DistributedIntCacheStream intCacheStream() {
      return new DistributedIntCacheStream(this);
   }
}
