package org.infinispan.stream.impl;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.container.entries.CacheEntry;
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
import org.infinispan.stream.impl.termop.primitive.ForEachDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachFlatMapObjDoubleOperation;
import org.infinispan.stream.impl.termop.primitive.ForEachObjDoubleOperation;
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

import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

/**
 * Implementation of {@link DoubleStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToDouble(ToDoubleFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 */
public class DistributedDoubleCacheStream extends AbstractCacheStream<Double, DoubleStream, DoubleCacheStream>
        implements DoubleCacheStream {
   /**
    * This constructor is to be used only when a user calls a map or flat map method changing to a DoubleStream
    * from a CacheStream, Stream, IntStream, LongStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedDoubleCacheStream(AbstractCacheStream other) {
      super(other);
   }

   @Override
   protected DoubleCacheStream unwrap() {
      return this;
   }

   @Override
   public DoubleCacheStream filter(DoublePredicate predicate) {
      return addIntermediateOperation(new FilterDoubleOperation(predicate));
   }

   @Override
   public DoubleCacheStream filter(SerializableDoublePredicate predicate) {
      return filter((DoublePredicate) predicate);
   }

   @Override
   public DoubleCacheStream map(DoubleUnaryOperator mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      return addIntermediateOperation(new MapDoubleOperation(mapper));
   }

   @Override
   public DoubleCacheStream map(SerializableDoubleUnaryOperator mapper) {
      return map((DoubleUnaryOperator) mapper);
   }

   @Override
   public <U> CacheStream<U> mapToObj(DoubleFunction<? extends U> mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToObjDoubleOperation<>(mapper));
      return cacheStream();
   }

   @Override
   public <U> CacheStream<U> mapToObj(SerializableDoubleFunction<? extends U> mapper) {
      return mapToObj((DoubleFunction<? extends U>) mapper);
   }

   @Override
   public IntCacheStream mapToInt(DoubleToIntFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToIntDoubleOperation(mapper));
      return intCacheStream();
   }

   @Override
   public IntCacheStream mapToInt(SerializableDoubleToIntFunction mapper) {
      return mapToInt((DoubleToIntFunction) mapper);
   }

   @Override
   public LongCacheStream mapToLong(DoubleToLongFunction mapper) {
      // Don't need to update iterator operation as we already are guaranteed to be at least MAP
      addIntermediateOperationMap(new MapToLongDoubleOperation(mapper));
      return longCacheStream();
   }

   @Override
   public LongCacheStream mapToLong(SerializableDoubleToLongFunction mapper) {
      return mapToLong((DoubleToLongFunction) mapper);
   }

   @Override
   public DoubleCacheStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperation(new FlatMapDoubleOperation(mapper));
   }

   @Override
   public DoubleCacheStream flatMap(SerializableDoubleFunction<? extends DoubleStream> mapper) {
      return flatMap((DoubleFunction<? extends DoubleStream>) mapper);
   }

   @Override
   public DoubleCacheStream distinct() {
      DistinctDoubleOperation op = DistinctDoubleOperation.getInstance();
      markDistinct(op, IntermediateType.DOUBLE);
      return addIntermediateOperation(op);
   }

   @Override
   public DoubleCacheStream sorted() {
      markSorted(IntermediateType.DOUBLE);
      return addIntermediateOperation(SortedDoubleOperation.getInstance());
   }

   @Override
   public DoubleCacheStream peek(DoubleConsumer action) {
      return addIntermediateOperation(new PeekDoubleOperation(action));
   }

   @Override
   public DoubleCacheStream peek(SerializableDoubleConsumer action) {
      return peek((DoubleConsumer) action);
   }

   @Override
   public DoubleCacheStream limit(long maxSize) {
      LimitDoubleOperation op = new LimitDoubleOperation(maxSize);
      markDistinct(op, IntermediateType.DOUBLE);
      return addIntermediateOperation(op);
   }

   @Override
   public DoubleCacheStream skip(long n) {
      SkipDoubleOperation op = new SkipDoubleOperation(n);
      markSkip(IntermediateType.DOUBLE);
      return addIntermediateOperation(op);
   }

   @Override
   public CacheStream<Double> boxed() {
      addIntermediateOperationMap(BoxedDoubleOperation.getInstance());
      return cacheStream();
   }

   // Rest are terminal operators

   @Override
   public void forEach(DoubleConsumer action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashKeyTrackingOperation(s -> getForEach(action, s));
      }
   }

   @Override
   public void forEach(SerializableDoubleConsumer action) {
      forEach((DoubleConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjDoubleConsumer<Cache<K, V>> action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashKeyTrackingOperation(s -> getForEach(action, s));
      }
   }

   @Override
   public <K, V> void forEach(SerializableObjDoubleConsumer<Cache<K, V>> action) {
      forEach((ObjDoubleConsumer<Cache<K, V>>) action);
   }

   KeyTrackingTerminalOperation<Object, Double, Object> getForEach(DoubleConsumer consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapDoubleOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachDoubleOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   <K, V> KeyTrackingTerminalOperation<Object, Double, Object> getForEach(ObjDoubleConsumer<Cache<K, V>> consumer,
           Supplier<Stream<CacheEntry>> supplier) {
      if (iteratorOperation == IteratorOperation.FLAT_MAP) {
         return new ForEachFlatMapObjDoubleOperation(intermediateOperations, supplier, distributedBatchSize, consumer);
      } else {
         return new ForEachObjDoubleOperation(intermediateOperations, supplier, distributedBatchSize, consumer);
      }
   }

   @Override
   public void forEachOrdered(DoubleConsumer action) {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         performIntermediateRemoteOperation((DoubleStream s) -> {
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
      return performOperation(TerminalFunctions.reduceFunction(identity, op), true, op::applyAsDouble, null);
   }

   @Override
   public double reduce(double identity, SerializableDoubleBinaryOperator op) {
      return reduce(identity, (DoubleBinaryOperator) op);
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
   public OptionalDouble reduce(SerializableDoubleBinaryOperator op) {
      return reduce((DoubleBinaryOperator) op);
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
   public <R> R collect(SerializableSupplier<R> supplier, SerializableObjDoubleConsumer<R> accumulator,
           SerializableBiConsumer<R, R> combiner) {
      return collect((Supplier<R>) supplier, accumulator, combiner);
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
         return OptionalDouble.of(results[0] / results[1]);
      } else {
         return OptionalDouble.empty();
      }
   }

   @Override
   public DoubleSummaryStatistics summaryStatistics() {
      return performOperation(TerminalFunctions.summaryStatisticsDoubleFunction(), true, (ds1, ds2) -> {
         ds1.combine(ds2);
         return ds1;
      }, null);
   }

   @Override
   public boolean anyMatch(DoublePredicate predicate) {
      return performOperation(TerminalFunctions.anyMatchFunction(predicate), false, Boolean::logicalOr, b -> b);
   }

   @Override
   public boolean anyMatch(SerializableDoublePredicate predicate) {
      return anyMatch((DoublePredicate) predicate);
   }

   @Override
   public boolean allMatch(DoublePredicate predicate) {
      return performOperation(TerminalFunctions.allMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean allMatch(SerializableDoublePredicate predicate) {
      return allMatch((DoublePredicate) predicate);
   }

   @Override
   public boolean noneMatch(DoublePredicate predicate) {
      return performOperation(TerminalFunctions.noneMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean noneMatch(SerializableDoublePredicate predicate) {
      return noneMatch((DoublePredicate) predicate);
   }

   @Override
   public OptionalDouble findFirst() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         return performIntermediateRemoteOperation((DoubleStream s) -> s.findFirst());
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
      return new DoubleIteratorToPrimitiveDouble(iterator);
   }

   static class DoubleIteratorToPrimitiveDouble implements PrimitiveIterator.OfDouble {
      private final Iterator<Double> iterator;

      DoubleIteratorToPrimitiveDouble(Iterator<Double> iterator) {
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
