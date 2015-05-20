package org.infinispan.stream.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.primitive.l.*;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapLongOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachLongOperation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Implementation of {@link LongStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToInt(ToIntFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 */
public class DistributedLongCacheStream extends AbstractCacheStream<Long, LongStream, LongConsumer>
        implements LongStream {
   /**
    * This constructor is to be used only when a user calls a map or flat map method changing to an IntStream
    * from a CacheStream, Stream, DoubleStream, IntStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedLongCacheStream(AbstractCacheStream other) {
      super(other);
   }

   @Override
   protected LongStream unwrap() {
      return this;
   }

   @Override
   public LongStream filter(LongPredicate predicate) {
      return addIntermediateOperation(new FilterLongOperation<>(predicate));
   }

   @Override
   public LongStream map(LongUnaryOperator mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperation(new MapLongOperation(mapper));
   }

   @Override
   public <U> Stream<U> mapToObj(LongFunction<? extends U> mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToObjLongOperation<>(mapper), cacheStream());
   }

   @Override
   public IntStream mapToInt(LongToIntFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToIntLongOperation(mapper), intCacheStream());
   }

   @Override
   public DoubleStream mapToDouble(LongToDoubleFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToDoubleLongOperation(mapper), doubleCacheStream());
   }

   @Override
   public LongStream flatMap(LongFunction<? extends LongStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperation(new FlatMapLongOperation(mapper));
   }

   @Override
   public LongStream distinct() {
      DistinctLongOperation op = DistinctLongOperation.getInstance();
      markDistinct(op, IntermediateType.LONG);
      return addIntermediateOperation(op);
   }

   @Override
   public LongStream sorted() {
      markSorted(IntermediateType.LONG);
      return addIntermediateOperation(SortedLongOperation.getInstance());
   }

   @Override
   public LongStream peek(LongConsumer action) {
      return addIntermediateOperation(new PeekLongOperation(action));
   }

   @Override
   public DoubleStream asDoubleStream() {
      return addIntermediateOperationMap(AsDoubleLongOperation.getInstance(), doubleCacheStream());
   }

   @Override
   public Stream<Long> boxed() {
      return addIntermediateOperationMap(BoxedLongOperation.getInstance(), cacheStream());
   }

   @Override
   public LongStream limit(long maxSize) {
      LimitLongOperation op = new LimitLongOperation(maxSize);
      markDistinct(op, IntermediateType.LONG);
      return addIntermediateOperation(op);
   }

   @Override
   public LongStream skip(long n) {
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
         performRehashForEach(action);
      }
   }

   @Override
   KeyTrackingTerminalOperation<Object, Long, Object> getForEach(LongConsumer consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapLongOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachLongOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   @Override
   public void forEachOrdered(LongConsumer action) {
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
      return performOperation(TerminalFunctions.reduceFunction(identity, op), true, (i1, i2) -> op.applyAsLong(i1, i2),
              null);
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
   public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return performOperation(TerminalFunctions.collectFunction(supplier, accumulator, combiner), true,
              (e1, e2) -> {
                 combiner.accept(e1, e2);
                 return e1;
              }, null);
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
      // TODO: maybe some day we can do this distributed way, currently IntSummaryStatistics is not serializable
      // and doesn't allow for creating one from given values.
      PrimitiveIterator.OfLong iterator = iterator();
      LongSummaryStatistics stats = new LongSummaryStatistics();
      iterator.forEachRemaining((long i) -> stats.accept(i));
      return stats;
   }

   @Override
   public boolean anyMatch(LongPredicate predicate) {
      return performOperation(TerminalFunctions.anyMatchFunction(predicate), false, Boolean::logicalOr, b -> b);
   }

   @Override
   public boolean allMatch(LongPredicate predicate) {
      return performOperation(TerminalFunctions.allMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean noneMatch(LongPredicate predicate) {
      return performOperation(TerminalFunctions.noneMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public OptionalLong findFirst() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         return performIntermediateRemoteOperation(s -> s.findFirst());
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
      return new LongIteratorToPrimiviteLong(iterator);
   }

   static class LongIteratorToPrimiviteLong implements PrimitiveIterator.OfLong {
      private final Iterator<Long> iterator;

      LongIteratorToPrimiviteLong(Iterator<Long> iterator) {
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
