package org.infinispan.stream.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.intops.primitive.d.*;
import org.infinispan.stream.impl.termop.primitive.ForEachDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapDoubleOperation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Implementation of {@link DoubleStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToDouble(ToDoubleFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 */
public class DistributedDoubleCacheStream extends AbstractCacheStream<Double, DoubleStream, DoubleConsumer>
        implements DoubleStream {
   /**
    * This constructor is to be used only when a user calls a map or flat map method changing to a DoubleStream
    * from a CacheStream, Stream, IntStream, LongStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedDoubleCacheStream(AbstractCacheStream other) {
      super(other);
   }

   @Override
   protected DoubleStream unwrap() {
      return this;
   }

   @Override
   public DoubleStream filter(DoublePredicate predicate) {
      return addIntermediateOperation(new FilterDoubleOperation(predicate));
   }

   @Override
   public DoubleStream map(DoubleUnaryOperator mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperation(new MapDoubleOperation(mapper));
   }

   @Override
   public <U> Stream<U> mapToObj(DoubleFunction<? extends U> mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToObjDoubleOperation<>(mapper), cacheStream());
   }

   @Override
   public IntStream mapToInt(DoubleToIntFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToIntDoubleOperation(mapper), intCacheStream());
   }

   @Override
   public LongStream mapToLong(DoubleToLongFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperationMap(new MapToLongDoubleOperation(mapper), longCacheStream());
   }

   @Override
   public DoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperation(new FlatMapDoubleOperation(mapper));
   }

   @Override
   public DoubleStream distinct() {
      DistinctDoubleOperation op = DistinctDoubleOperation.getInstance();
      markDistinct(op, IntermediateType.DOUBLE);
      return addIntermediateOperation(op);
   }

   @Override
   public DoubleStream sorted() {
      markSorted(IntermediateType.DOUBLE);
      return addIntermediateOperation(SortedDoubleOperation.getInstance());
   }

   @Override
   public DoubleStream peek(DoubleConsumer action) {
      return addIntermediateOperation(new PeekDoubleOperation(action));
   }

   @Override
   public DoubleStream limit(long maxSize) {
      LimitDoubleOperation op = new LimitDoubleOperation(maxSize);
      markDistinct(op, IntermediateType.DOUBLE);
      return addIntermediateOperation(op);
   }

   @Override
   public DoubleStream skip(long n) {
      SkipDoubleOperation op = new SkipDoubleOperation(n);
      markSkip(IntermediateType.DOUBLE);
      return addIntermediateOperation(op);
   }

   @Override
   public Stream<Double> boxed() {
      return addIntermediateOperationMap(BoxedDoubleOperation.getInstance(), cacheStream());
   }

   // Rest are terminal operators

   @Override
   public void forEach(DoubleConsumer action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashForEach(action);
      }
   }

   @Override
   KeyTrackingTerminalOperation<Object, Double, Object> getForEach(DoubleConsumer consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapDoubleOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachDoubleOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   @Override
   public void forEachOrdered(DoubleConsumer action) {
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
   public double[] toArray() {
      return performOperation(TerminalFunctions.toArrayDoubleFunction(), false,
              (v1, v2) -> {
                 double[] array = Arrays.copyOf(v1, v1.length + v2.length);
                 System.arraycopy(v2, 0, array, v1.length, v2.length);
                 return array;
              }, null, false);
   }

   @Override
   public double reduce(double identity, DoubleBinaryOperator op) {
      return performOperation(TerminalFunctions.reduceFunction(identity, op), true,
              (i1, i2) -> op.applyAsDouble(i1, i2), null);
   }

   @Override
   public OptionalDouble reduce(DoubleBinaryOperator op) {
      Double result = performOperation(TerminalFunctions.reduceFunction(op), true,
              (i1, i2) -> {
                 if (i1 != null) {
                    if (i2 != null) {
                       return op.applyAsDouble(i1, i2);
                    }
                    return i1;
                 }
                 return i2;
              }, null);
      if (result == null) {
         return OptionalDouble.empty();
      } else {
         return OptionalDouble.of(result);
      }
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return performOperation(TerminalFunctions.collectFunction(supplier, accumulator, combiner), true,
              (e1, e2) -> {
                 combiner.accept(e1, e2);
                 return e1;
              }, null);
   }

   @Override
   public double sum() {
      return performOperation(TerminalFunctions.sumDoubleFunction(), true, (i1, i2) -> i1 + i2, null);
   }

   @Override
   public OptionalDouble min() {
      Double value = performOperation(TerminalFunctions.minDoubleFunction(), false,
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
         return OptionalDouble.empty();
      } else {
         return OptionalDouble.of(value);
      }
   }

   @Override
   public OptionalDouble max() {
      Double value = performOperation(TerminalFunctions.maxDoubleFunction(), false,
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
         return OptionalDouble.empty();
      } else {
         return OptionalDouble.of(value);
      }
   }

   @Override
   public OptionalDouble average() {
      double[] results = performOperation(TerminalFunctions.averageDoubleFunction(), true,
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
   public DoubleSummaryStatistics summaryStatistics() {
      // TODO: maybe some day we can do this distributed way, currently IntSummaryStatistics is not serializable
      // and doesn't allow for creating one from given values.
      PrimitiveIterator.OfDouble iterator = iterator();
      DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
      iterator.forEachRemaining((double i) -> stats.accept(i));
      return stats;
   }

   @Override
   public boolean anyMatch(DoublePredicate predicate) {
      return performOperation(TerminalFunctions.anyMatchFunction(predicate), false, Boolean::logicalOr, b -> b);
   }

   @Override
   public boolean allMatch(DoublePredicate predicate) {
      return performOperation(TerminalFunctions.allMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean noneMatch(DoublePredicate predicate) {
      return performOperation(TerminalFunctions.noneMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public OptionalDouble findFirst() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         return performIntermediateRemoteOperation(s -> s.findFirst());
      } else {
         return findAny();
      }
   }

   @Override
   public OptionalDouble findAny() {
      Double result = performOperation(TerminalFunctions.findAnyDoubleFunction(), false,
              (i1, i2) -> {
                 if (i1 != null) {
                    return i1;
                 } else {
                    return i2;
                 }
              }, a -> a != null);
      if (result != null) {
         return OptionalDouble.of(result);
      } else {
         return OptionalDouble.empty();
      }
   }

   @Override
   public PrimitiveIterator.OfDouble iterator() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         DoubleStream stream = performIntermediateRemoteOperation(Function.identity());
         return stream.iterator();
      } else {
         return remoteIterator();
      }
   }

   PrimitiveIterator.OfDouble remoteIterator() {
      // TODO: need to add in way to not box these later
      // Since this is a remote iterator we have to add it to the remote intermediate operations queue
      intermediateOperations.add(BoxedDoubleOperation.getInstance());
      DistributedCacheStream<Double> stream = new DistributedCacheStream<>(this);
      Iterator<Double> iterator = stream.remoteIterator();
      return new DoubleIteratorToPrimiviteDouble(iterator);
   }

   static class DoubleIteratorToPrimiviteDouble implements PrimitiveIterator.OfDouble {
      private final Iterator<Double> iterator;

      DoubleIteratorToPrimiviteDouble(Iterator<Double> iterator) {
         this.iterator = iterator;
      }

      @Override
      public double nextDouble() {
         return iterator.next();
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }
   }

   @Override
   public Spliterator.OfDouble spliterator() {
      return Spliterators.spliteratorUnknownSize(iterator(), 0);
   }

   @Override
   public long count() {
      return performOperation(TerminalFunctions.countDoubleFunction(), true, (i1, i2) -> i1 + i2, null);
   }

   protected <R> DistributedCacheStream<R> cacheStream() {
      return new DistributedCacheStream<>(this);
   }

   protected DistributedIntCacheStream intCacheStream() {
      return new DistributedIntCacheStream(this);
   }

   protected DistributedLongCacheStream longCacheStream() {
      return new DistributedLongCacheStream(this);
   }
}
