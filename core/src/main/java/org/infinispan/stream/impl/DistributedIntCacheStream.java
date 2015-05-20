package org.infinispan.stream.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.primitive.i.*;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapIntOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachIntOperation;

import java.io.Serializable;
import java.util.*;
import java.util.function.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Implementation of {@link IntStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToInt(ToIntFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 */
public class DistributedIntCacheStream extends AbstractCacheStream<Integer, IntStream, IntConsumer>
        implements IntStream {
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
   public IntStream filter(IntPredicate predicate) {
      return addIntermediateOperation(new FilterIntOperation<>(predicate));
   }

   @Override
   public IntStream map(IntUnaryOperator mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperation(new MapIntOperation(mapper));
   }

   @Override
   public <U> Stream<U> mapToObj(IntFunction<? extends U> mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToObjIntOperation<>(mapper), cacheStream());
   }

   @Override
   public LongStream mapToLong(IntToLongFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToLongIntOperation(mapper), longCacheStream());
   }

   @Override
   public DoubleStream mapToDouble(IntToDoubleFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToDoubleIntOperation(mapper), doubleCacheStream());
   }

   @Override
   public IntStream flatMap(IntFunction<? extends IntStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperation(new FlatMapIntOperation(mapper));
   }

   @Override
   public IntStream distinct() {
      DistinctIntOperation op = DistinctIntOperation.getInstance();
      markDistinct(op, IntermediateType.INT);
      return addIntermediateOperation(op);
   }

   @Override
   public IntStream sorted() {
      markSorted(IntermediateType.INT);
      return addIntermediateOperation(SortedIntOperation.getInstance());
   }

   @Override
   public IntStream peek(IntConsumer action) {
      return addIntermediateOperation(new PeekIntOperation(action));
   }

   @Override
   public IntStream limit(long maxSize) {
      LimitIntOperation op = new LimitIntOperation(maxSize);
      markDistinct(op, IntermediateType.INT);
      return addIntermediateOperation(op);
   }

   @Override
   public IntStream skip(long n) {
      SkipIntOperation op = new SkipIntOperation(n);
      markSkip(IntermediateType.INT);
      return addIntermediateOperation(op);
   }

   @Override
   public LongStream asLongStream() {
      return addIntermediateOperationMap(AsLongIntOperation.getInstance(), longCacheStream());
   }

   @Override
   public DoubleStream asDoubleStream() {
      return addIntermediateOperationMap(AsDoubleIntOperation.getInstance(), doubleCacheStream());
   }

   @Override
   public Stream<Integer> boxed() {
      return addIntermediateOperationMap(BoxedIntOperation.getInstance(), cacheStream());
   }

   // Rest are terminal operators


   @Override
   public void forEach(IntConsumer action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashForEach(action);
      }
   }

   @Override
   KeyTrackingTerminalOperation<Object, Integer, Object> getForEach(IntConsumer consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapIntOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachIntOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   @Override
   public void forEachOrdered(IntConsumer action) {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         performIntermediateRemoteOperation(s -> {
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
      return performOperation(TerminalFunctions.reduceFunction(identity, op), true, (i1, i2) -> op.applyAsInt(i1, i2),
              null);
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
   public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return performOperation(TerminalFunctions.collectFunction(supplier, accumulator, combiner), true,
              (e1, e2) -> {
                 combiner.accept(e1, e2);
                 return e1;
              }, null);
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
      // TODO: maybe some day we can do this distributed way, currently IntSummaryStatistics is not serializable
      // and doesn't allow for creating one from given values.
      PrimitiveIterator.OfInt iterator = iterator();
      IntSummaryStatistics stats = new IntSummaryStatistics();
      iterator.forEachRemaining((int i) -> stats.accept(i));
      return stats;
   }

   @Override
   public boolean anyMatch(IntPredicate predicate) {
      return performOperation(TerminalFunctions.allMatchFunction(predicate), false, Boolean::logicalOr, b -> b);
   }

   @Override
   public boolean allMatch(IntPredicate predicate) {
      return performOperation(TerminalFunctions.anyMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean noneMatch(IntPredicate predicate) {
      return performOperation(TerminalFunctions.noneMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public OptionalInt findFirst() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         return performIntermediateRemoteOperation(s -> s.findFirst());
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
      return new IntegerIteratorToPrimiviteInteger(iterator);
   }

   static class IntegerIteratorToPrimiviteInteger implements PrimitiveIterator.OfInt {
      private final Iterator<Integer> iterator;

      IntegerIteratorToPrimiviteInteger(Iterator<Integer> iterator) {
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
