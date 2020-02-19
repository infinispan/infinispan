package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.DoubleSummaryStatistics;
import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.reactive.publisher.PublisherReducers;
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
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableBinaryOperator;
import org.infinispan.util.function.SerializableCallable;
import org.infinispan.util.function.SerializableComparator;
import org.infinispan.util.function.SerializableDoubleConsumer;
import org.infinispan.util.function.SerializableDoubleFunction;
import org.infinispan.util.function.SerializableDoublePredicate;
import org.infinispan.util.function.SerializableDoubleToIntFunction;
import org.infinispan.util.function.SerializableDoubleToLongFunction;
import org.infinispan.util.function.SerializableDoubleUnaryOperator;
import org.infinispan.util.function.SerializableObjDoubleConsumer;
import org.infinispan.util.function.SerializablePredicate;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

/**
 * Implementation of {@link DoubleStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToDouble(ToDoubleFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 * @param <Original> original stream type
 */
public class DistributedDoubleCacheStream<Original> extends AbstractCacheStream<Original, Double, DoubleStream, DoubleCacheStream>
        implements DoubleCacheStream {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   /**
    * This constructor is to be used only when a user calls a map or flat map method changing to a DoubleStream
    * from a CacheStream, Stream, IntStream, LongStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedDoubleCacheStream(AbstractCacheStream other) {
      super(other);
   }

   @Override
   protected Log getLog() {
      return log;
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
      // Limit is applied remotely as well
      addIntermediateOperation(DistinctDoubleOperation.getInstance());
      return new IntermediateDoubleCacheStream(this).distinct();
   }

   @Override
   public DoubleCacheStream sorted() {
      return new IntermediateDoubleCacheStream(this).sorted();
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
      // Limit is applied remotely as well
      addIntermediateOperation(new LimitDoubleOperation(maxSize));
      return new IntermediateDoubleCacheStream(this).limit(maxSize);
   }

   @Override
   public DoubleCacheStream skip(long n) {
      return new IntermediateDoubleCacheStream(this).skip(n);
   }

   @Override
   public CacheStream<Double> boxed() {
      addIntermediateOperationMap(BoxedDoubleOperation.getInstance());
      return cacheStream();
   }

   // Rest are terminal operators

   @Override
   public void forEach(DoubleConsumer action) {
      peek(action)
         .iterator()
            .forEachRemaining((double ignore) -> { });
   }

   @Override
   public void forEach(SerializableDoubleConsumer action) {
      forEach((DoubleConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjDoubleConsumer<Cache<K, V>> action) {
      peek(CacheBiConsumers.doubleConsumer(action))
            .iterator()
            .forEachRemaining((double ignore) -> { });
   }

   @Override
   public <K, V> void forEach(SerializableObjDoubleConsumer<Cache<K, V>> action) {
      forEach((ObjDoubleConsumer<Cache<K, V>>) action);
   }

   @Override
   public void forEachOrdered(DoubleConsumer action) {
      // We aren't sorted, so just do forEach
      forEach(action);
   }

   @Override
   public double[] toArray() {
      Object[] values = performPublisherOperation(PublisherReducers.toArrayReducer(), PublisherReducers.toArrayFinalizer());

      double[] results = new double[values.length];
      int i = 0;
      for (Object obj : values) {
         results[i++] = (Double) obj;
      }
      return results;
   }

   @Override
   public double reduce(double identity, DoubleBinaryOperator op) {
      Function<Publisher<Double>, CompletionStage<Double>> reduce = PublisherReducers.reduce(identity,
            (SerializableBiFunction<Double, Double, Double>) op::applyAsDouble);
      return performPublisherOperation(reduce, reduce);
   }

   @Override
   public OptionalDouble reduce(DoubleBinaryOperator op) {
      Function<Publisher<Double>, CompletionStage<Double>> reduce = PublisherReducers.reduce(
            (SerializableBinaryOperator<Double>) op::applyAsDouble);
      Double result = performPublisherOperation(reduce, reduce);
      if (result == null) {
         return OptionalDouble.empty();
      }
      return OptionalDouble.of(result);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return performPublisherOperation(PublisherReducers.collect(supplier,
            (SerializableBiConsumer<R, Double>) accumulator::accept),
            PublisherReducers.accumulate(combiner));
   }

   @Override
   public double sum() {
      DoubleSummaryStatistics dss = summaryStatistics();
      return dss.getSum();
   }

   @Override
   public OptionalDouble min() {
      SerializableComparator<Double> serializableComparator = Double::compareTo;
      Function<Publisher<Double>, CompletionStage<Double>> minFunction = PublisherReducers.min(serializableComparator);
      Double min = performPublisherOperation(minFunction, minFunction);
      if (min == null) {
         return OptionalDouble.empty();
      }
      return OptionalDouble.of(min);
   }

   @Override
   public OptionalDouble max() {
      SerializableComparator<Double> serializableComparator = Double::compareTo;
      Function<Publisher<Double>, CompletionStage<Double>> maxFunction = PublisherReducers.max(serializableComparator);
      Double max = performPublisherOperation(maxFunction, maxFunction);
      if (max == null) {
         return OptionalDouble.empty();
      }
      return OptionalDouble.of(max);
   }

   @Override
   public OptionalDouble average() {
      DoubleSummaryStatistics dss = summaryStatistics();
      if (dss.getCount() == 0) {
         return OptionalDouble.empty();
      }
      return OptionalDouble.of(dss.getAverage());
   }

   @Override
   public DoubleSummaryStatistics summaryStatistics() {
      return performPublisherOperation(PublisherReducers.reduceWith(
            (SerializableCallable<DoubleSummaryStatistics>) DoubleSummaryStatistics::new,
            (SerializableBiFunction<DoubleSummaryStatistics, Double, DoubleSummaryStatistics>) (dss, doubleValue) -> {
               dss.accept(doubleValue);
               return dss;
            }), PublisherReducers.reduce(
            (SerializableBinaryOperator<DoubleSummaryStatistics>) (first, second) -> {
               first.combine(second);
               return first;
            }));
   }

   @Override
   public boolean anyMatch(DoublePredicate predicate) {
      return performPublisherOperation(PublisherReducers.anyMatch((SerializablePredicate<Double>) predicate::test),
            PublisherReducers.or());
   }

   @Override
   public boolean allMatch(DoublePredicate predicate) {
      return performPublisherOperation(PublisherReducers.allMatch((SerializablePredicate<Double>) predicate::test),
            PublisherReducers.and());
   }

   @Override
   public boolean noneMatch(DoublePredicate predicate) {
      return performPublisherOperation(PublisherReducers.noneMatch((SerializablePredicate<Double>) predicate::test),
            PublisherReducers.and());
   }

   @Override
   public OptionalDouble findFirst() {
      // We aren't sorted, so just do findAny
      return findAny();
   }

   @Override
   public OptionalDouble findAny() {
      Function<Publisher<Double>, CompletionStage<Double>> function = PublisherReducers.findFirst();
      Double value = performPublisherOperation(function, function);
      if (value == null) {
         return OptionalDouble.empty();
      }
      return OptionalDouble.of(value);
   }

   @Override
   public PrimitiveIterator.OfDouble iterator() {
      return remoteIterator();
   }

   PrimitiveIterator.OfDouble remoteIterator() {
      // TODO: need to add in way to not box these later
      // Since this is a remote iterator we have to add it to the remote intermediate operations queue
      intermediateOperations.add(BoxedDoubleOperation.getInstance());
      DistributedCacheStream<Original, Double> stream = new DistributedCacheStream<>(this);
      Iterator<Double> iterator = stream.iterator();
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
      return performPublisherOperation(PublisherReducers.count(), PublisherReducers.add());
   }

   // These are the custom added methods for cache streams

   @Override
   public DoubleCacheStream sequentialDistribution() {
      parallelDistribution = false;
      return this;
   }

   @Override
   public DoubleCacheStream parallelDistribution() {
      parallelDistribution = true;
      return this;
   }

   @Override
   public DoubleCacheStream filterKeySegments(Set<Integer> segments) {
      return filterKeySegments(IntSets.from(segments));
   }

   @Override
   public DoubleCacheStream filterKeySegments(IntSet segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public DoubleCacheStream filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public DoubleCacheStream distributedBatchSize(int batchSize) {
      distributedBatchSize = batchSize;
      return this;
   }

   @Override
   public DoubleCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      if (segmentCompletionListener == null) {
         segmentCompletionListener = listener;
      } else {
         segmentCompletionListener = composeWithExceptions(segmentCompletionListener, listener);
      }
      return this;
   }

   @Override
   public DoubleCacheStream disableRehashAware() {
      rehashAware = false;
      return this;
   }

   @Override
   public DoubleCacheStream timeout(long timeout, TimeUnit unit) {
      if (timeout <= 0) {
         throw new IllegalArgumentException("Timeout must be greater than 0");
      }
      this.timeout = timeout;
      this.timeoutUnit = unit;
      return this;
   }

   protected <R> DistributedCacheStream<Original, R> cacheStream() {
      return new DistributedCacheStream<>(this);
   }

   protected DistributedIntCacheStream<Original> intCacheStream() {
      return new DistributedIntCacheStream<>(this);
   }

   protected DistributedLongCacheStream<Original> longCacheStream() {
      return new DistributedLongCacheStream<>(this);
   }
}
