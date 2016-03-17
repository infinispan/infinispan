package org.infinispan.stream.impl;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.primitive.i.AsDoubleIntOperation;
import org.infinispan.stream.impl.intops.primitive.i.AsLongIntOperation;
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
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapObjIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachObjIntOperation;
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

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implementation of {@link IntStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToInt(ToIntFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 */
public class DistributedIntCacheStream extends AbstractCacheStream<Integer, IntStream, IntCacheStream>
        implements IntCacheStream {
   /**
    * This constructor is to be used only when a user calls a map or flat map method changing to an IntStream
    * from a CacheStream, Stream, DoubleStream, LongStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedIntCacheStream(AbstractCacheStream other) {
      super(other);
   }

   @Override
   protected DistributedIntCacheStream unwrap() {
      return this;
   }

   @Override
   public IntCacheStream filter(IntPredicate predicate) {
      return addIntermediateOperation(new FilterIntOperation<>(predicate));
   }

   @Override
   public IntCacheStream filter(SerializableIntPredicate predicate) {
      return filter((IntPredicate) predicate);
   }

   @Override
   public IntCacheStream map(IntUnaryOperator mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperation(new MapIntOperation(mapper));
   }

   @Override
   public IntCacheStream map(SerializableIntUnaryOperator mapper) {
      return map((IntUnaryOperator) mapper);
   }

   @Override
   public <U> CacheStream<U> mapToObj(IntFunction<? extends U> mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToObjIntOperation<>(mapper));
      return cacheStream();
   }

   @Override
   public <U> CacheStream<U> mapToObj(SerializableIntFunction<? extends U> mapper) {
      return mapToObj((IntFunction<? extends U>) mapper);
   }

   @Override
   public LongCacheStream mapToLong(IntToLongFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToLongIntOperation(mapper));
      return longCacheStream();
   }

   @Override
   public LongCacheStream mapToLong(SerializableIntToLongFunction mapper) {
      return mapToLong((IntToLongFunction) mapper);
   }

   @Override
   public DoubleCacheStream mapToDouble(IntToDoubleFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToDoubleIntOperation(mapper));
      return doubleCacheStream();
   }

   @Override
   public DoubleCacheStream mapToDouble(SerializableIntToDoubleFunction mapper) {
      return mapToDouble((IntToDoubleFunction) mapper);
   }

   @Override
   public IntCacheStream flatMap(IntFunction<? extends IntStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperation(new FlatMapIntOperation(mapper));
   }

   @Override
   public IntCacheStream flatMap(SerializableIntFunction<? extends IntStream> mapper) {
      return flatMap((IntFunction<? extends IntStream>) mapper);
   }

   @Override
   public IntCacheStream distinct() {
      DistinctIntOperation op = DistinctIntOperation.getInstance();
      markDistinct(op, IntermediateType.INT);
      return addIntermediateOperation(op);
   }

   @Override
   public IntCacheStream sorted() {
      markSorted(IntermediateType.INT);
      return addIntermediateOperation(SortedIntOperation.getInstance());
   }

   @Override
   public IntCacheStream peek(IntConsumer action) {
      return addIntermediateOperation(new PeekIntOperation(action));
   }

   @Override
   public IntCacheStream peek(SerializableIntConsumer action) {
      return peek((IntConsumer) action);
   }

   @Override
   public IntCacheStream limit(long maxSize) {
      LimitIntOperation op = new LimitIntOperation(maxSize);
      markDistinct(op, IntermediateType.INT);
      return addIntermediateOperation(op);
   }

   @Override
   public IntCacheStream skip(long n) {
      SkipIntOperation op = new SkipIntOperation(n);
      markSkip(IntermediateType.INT);
      return addIntermediateOperation(op);
   }

   @Override
   public LongCacheStream asLongStream() {
      addIntermediateOperationMap(AsLongIntOperation.getInstance());
      return longCacheStream();
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      addIntermediateOperationMap(AsDoubleIntOperation.getInstance());
      return doubleCacheStream();
   }

   @Override
   public CacheStream<Integer> boxed() {
      addIntermediateOperationMap(BoxedIntOperation.getInstance());
      return cacheStream();
   }

   // Rest are terminal operators


   @Override
   public void forEach(IntConsumer action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashKeyTrackingOperation(s -> getForEach(action, s));
      }
   }

   @Override
   public void forEach(SerializableIntConsumer action) {
      forEach((IntConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjIntConsumer<Cache<K, V>> action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashKeyTrackingOperation(s -> getForEach(action, s));
      }
   }

   @Override
   public <K, V> void forEach(SerializableObjIntConsumer<Cache<K, V>> action) {
      forEach((ObjIntConsumer<Cache<K, V>>) action);
   }

   KeyTrackingTerminalOperation<Object, Integer, Object> getForEach(IntConsumer consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapIntOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachIntOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   <K, V> KeyTrackingTerminalOperation<Object, Integer, Object> getForEach(ObjIntConsumer<Cache<K, V>> consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapObjIntOperation(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachObjIntOperation(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   @Override
   public void forEachOrdered(IntConsumer action) {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         performIntermediateRemoteOperation((IntStream s) -> {
            s.forEachOrdered(action);
            return null;
         });
      } else {
         forEach(action);
      }
   }

   @Override
   public int[] toArray() {
      return performOperation(TerminalFunctions.toArrayIntFunction(), false,
              (v1, v2) -> {
                 int[] array = Arrays.copyOf(v1, v1.length + v2.length);
                 System.arraycopy(v2, 0, array, v1.length, v2.length);
                 return array;
              }, null, false);
   }

   @Override
   public int reduce(int identity, IntBinaryOperator op) {
      return performOperation(TerminalFunctions.reduceFunction(identity, op), true, op::applyAsInt, null);
   }

   @Override
   public int reduce(int identity, SerializableIntBinaryOperator op) {
      return reduce(identity, (IntBinaryOperator) op);
   }

   @Override
   public OptionalInt reduce(IntBinaryOperator op) {
      Integer result = performOperation(TerminalFunctions.reduceFunction(op), true,
              (i1, i2) -> {
                 if (i1 != null) {
                    if (i2 != null) {
                       return op.applyAsInt(i1, i2);
                    }
                    return i1;
                 }
                 return i2;
              }, null);
      if (result == null) {
         return OptionalInt.empty();
      } else {
         return OptionalInt.of(result);
      }
   }

   @Override
   public OptionalInt reduce(SerializableIntBinaryOperator op) {
      return reduce((IntBinaryOperator) op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return performOperation(TerminalFunctions.collectFunction(supplier, accumulator, combiner), true,
              (e1, e2) -> {
                 combiner.accept(e1, e2);
                 return e1;
              }, null);
   }

   @Override
   public <R> R collect(SerializableSupplier<R> supplier, SerializableObjIntConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
   }

   @Override
   public int sum() {
      return performOperation(TerminalFunctions.sumIntFunction(), true, (i1, i2) -> i1 + i2, null);
   }

   @Override
   public OptionalInt min() {
      Integer value = performOperation(TerminalFunctions.minIntFunction(), false,
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
         return OptionalInt.empty();
      } else {
         return OptionalInt.of(value);
      }
   }

   @Override
   public OptionalInt max() {
      Integer value = performOperation(TerminalFunctions.maxIntFunction(), false,
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
         return OptionalInt.empty();
      } else {
         return OptionalInt.of(value);
      }
   }

   @Override
   public OptionalDouble average() {
      long[] results = performOperation(TerminalFunctions.averageIntFunction(), true,
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
   public IntSummaryStatistics summaryStatistics() {
      return performOperation(TerminalFunctions.summaryStatisticsIntFunction(), true, (is1, is2) -> {
         is1.combine(is2);
         return is1;
      }, null);
   }

   @Override
   public boolean anyMatch(IntPredicate predicate) {
      return performOperation(TerminalFunctions.anyMatchFunction(predicate), false, Boolean::logicalOr, b -> b);
   }

   @Override
   public boolean anyMatch(SerializableIntPredicate predicate) {
      return anyMatch((IntPredicate) predicate);
   }

   @Override
   public boolean allMatch(IntPredicate predicate) {
      return performOperation(TerminalFunctions.allMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean allMatch(SerializableIntPredicate predicate) {
      return allMatch((IntPredicate) predicate);
   }

   @Override
   public boolean noneMatch(IntPredicate predicate) {
      return performOperation(TerminalFunctions.noneMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean noneMatch(SerializableIntPredicate predicate) {
      return noneMatch((IntPredicate) predicate);
   }

   @Override
   public OptionalInt findFirst() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         return performIntermediateRemoteOperation((IntStream s) -> s.findFirst());
      } else {
         return findAny();
      }
   }

   @Override
   public OptionalInt findAny() {
      Integer result = performOperation(TerminalFunctions.findAnyIntFunction(), false,
              (i1, i2) -> {
                 if (i1 != null) {
                    return i1;
                 } else {
                    return i2;
                 }
              }, a -> a != null);
      if (result != null) {
         return OptionalInt.of(result);
      } else {
         return OptionalInt.empty();
      }
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         IntStream stream = performIntermediateRemoteOperation(Function.identity());
         return stream.iterator();
      } else {
         return remoteIterator();
      }
   }

   PrimitiveIterator.OfInt remoteIterator() {
      // TODO: need to add in way to not box these later
      // Since this is a remote iterator we have to add it to the remote intermediate operations queue
      intermediateOperations.add(BoxedIntOperation.getInstance());
      DistributedCacheStream<Integer> stream = new DistributedCacheStream<>(this);
      Iterator<Integer> iterator = stream.remoteIterator();
      return new IntegerIteratorToPrimitiveInteger(iterator);
   }

   static class IntegerIteratorToPrimitiveInteger implements PrimitiveIterator.OfInt {
      private final Iterator<Integer> iterator;

      IntegerIteratorToPrimitiveInteger(Iterator<Integer> iterator) {
         this.iterator = iterator;
      }

      @Override
      public int nextInt() {
         return iterator.next();
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }
   }

   @Override
   public Spliterator.OfInt spliterator() {
      return Spliterators.spliteratorUnknownSize(iterator(), Spliterator.CONCURRENT);
   }

   @Override
   public long count() {
      return performOperation(TerminalFunctions.countIntFunction(), true, (i1, i2) -> i1 + i2, null);
   }

   protected <R> DistributedCacheStream<R> cacheStream() {
      return new DistributedCacheStream<>(this);
   }

   protected DistributedDoubleCacheStream doubleCacheStream() {
      return new DistributedDoubleCacheStream(this);
   }

   protected DistributedLongCacheStream longCacheStream() {
      return new DistributedLongCacheStream(this);
   }
}
