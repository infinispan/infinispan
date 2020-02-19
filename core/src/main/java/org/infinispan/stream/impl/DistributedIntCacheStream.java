package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.reactive.publisher.PublisherReducers;
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
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableBinaryOperator;
import org.infinispan.util.function.SerializableCallable;
import org.infinispan.util.function.SerializableComparator;
import org.infinispan.util.function.SerializableIntConsumer;
import org.infinispan.util.function.SerializableIntFunction;
import org.infinispan.util.function.SerializableIntPredicate;
import org.infinispan.util.function.SerializableIntToDoubleFunction;
import org.infinispan.util.function.SerializableIntToLongFunction;
import org.infinispan.util.function.SerializableIntUnaryOperator;
import org.infinispan.util.function.SerializableObjIntConsumer;
import org.infinispan.util.function.SerializablePredicate;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

/**
 * Implementation of {@link IntStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToInt(ToIntFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 * @param <Original> original stream type
 */
public class DistributedIntCacheStream<Original> extends AbstractCacheStream<Original, Integer, IntStream, IntCacheStream>
        implements IntCacheStream {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   /**
    * This constructor is to be used only when a user calls a map or flat map method changing to an IntStream
    * from a CacheStream, Stream, DoubleStream, LongStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedIntCacheStream(AbstractCacheStream other) {
      super(other);
   }

   @Override
   protected Log getLog() {
      return log;
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
      // Distinct is applied remotely as well
      addIntermediateOperation(DistinctIntOperation.getInstance());
      return new IntermediateIntCacheStream(this).distinct();
   }

   @Override
   public IntCacheStream sorted() {
      return new IntermediateIntCacheStream(this).sorted();
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
      // Limit is applied remotely as well
      addIntermediateOperation(new LimitIntOperation(maxSize));
      return new IntermediateIntCacheStream(this).limit(maxSize);
   }

   @Override
   public IntCacheStream skip(long n) {
      return new IntermediateIntCacheStream(this).skip(n);
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
      peek(action)
            .iterator()
            .forEachRemaining((int ignore) -> { });
   }

   @Override
   public void forEach(SerializableIntConsumer action) {
      forEach((IntConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjIntConsumer<Cache<K, V>> action) {
      peek(CacheBiConsumers.intConsumer(action))
            .iterator()
            .forEachRemaining((int ignore) -> { });
   }

   @Override
   public <K, V> void forEach(SerializableObjIntConsumer<Cache<K, V>> action) {
      forEach((ObjIntConsumer<Cache<K, V>>) action);
   }

   @Override
   public void forEachOrdered(IntConsumer action) {
      // We aren't sorted, so just do forEach
      forEach(action);
   }

   @Override
   public int[] toArray() {
      Object[] values = performPublisherOperation(PublisherReducers.toArrayReducer(), PublisherReducers.toArrayFinalizer());

      int[] results = new int[values.length];
      int i = 0;
      for (Object obj : values) {
         results[i++] = (Integer) obj;
      }
      return results;
   }

   @Override
   public int reduce(int identity, IntBinaryOperator op) {
      Function<Publisher<Integer>, CompletionStage<Integer>> reduce = PublisherReducers.reduce(identity,
            (SerializableBiFunction<Integer, Integer, Integer>) op::applyAsInt);
      return performPublisherOperation(reduce, reduce);
   }

   @Override
   public OptionalInt reduce(IntBinaryOperator op) {
      Function<Publisher<Integer>, CompletionStage<Integer>> reduce = PublisherReducers.reduce(
            (SerializableBinaryOperator<Integer>) op::applyAsInt);
      Integer result = performPublisherOperation(reduce, reduce);
      if (result == null) {
         return OptionalInt.empty();
      }
      return OptionalInt.of(result);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return performPublisherOperation(PublisherReducers.collect(supplier,
            (SerializableBiConsumer<R, Integer>) accumulator::accept),
            PublisherReducers.accumulate(combiner));
   }

   @Override
   public int sum() {
      long result = mapToLong(Integer::toUnsignedLong).sum();
      if (result > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      }
      return (int) result;
   }

   @Override
   public OptionalInt min() {
      SerializableComparator<Integer> serializableComparator = Integer::compareTo;
      Function<Publisher<Integer>, CompletionStage<Integer>> minFunction = PublisherReducers.min(serializableComparator);
      Integer min = performPublisherOperation(minFunction, minFunction);
      if (min == null) {
         return OptionalInt.empty();
      }
      return OptionalInt.of(min);
   }

   @Override
   public OptionalInt max() {
      SerializableComparator<Integer> serializableComparator = Integer::compareTo;
      Function<Publisher<Integer>, CompletionStage<Integer>> maxFunction = PublisherReducers.max(serializableComparator);
      Integer max = performPublisherOperation(maxFunction, maxFunction);
      if (max == null) {
         return OptionalInt.empty();
      }
      return OptionalInt.of(max);
   }

   @Override
   public OptionalDouble average() {
      IntSummaryStatistics iss = summaryStatistics();
      if (iss.getCount() == 0) {
         return OptionalDouble.empty();
      }
      return OptionalDouble.of(iss.getAverage());
   }

   @Override
   public IntSummaryStatistics summaryStatistics() {
      return performPublisherOperation(PublisherReducers.reduceWith(
            (SerializableCallable<IntSummaryStatistics>) IntSummaryStatistics::new,
            (SerializableBiFunction<IntSummaryStatistics, Integer, IntSummaryStatistics>) (lss, intValue) -> {
               lss.accept(intValue);
               return lss;
            }), PublisherReducers.reduce(
            (SerializableBinaryOperator<IntSummaryStatistics>) (first, second) -> {
               first.combine(second);
               return first;
            }));
   }

   @Override
   public boolean anyMatch(IntPredicate predicate) {
      return performPublisherOperation(PublisherReducers.anyMatch((SerializablePredicate<Integer>) predicate::test),
            PublisherReducers.or());
   }

   @Override
   public boolean allMatch(IntPredicate predicate) {
      return performPublisherOperation(PublisherReducers.allMatch((SerializablePredicate<Integer>) predicate::test),
            PublisherReducers.and());
   }

   @Override
   public boolean noneMatch(IntPredicate predicate) {
      return performPublisherOperation(PublisherReducers.noneMatch((SerializablePredicate<Integer>) predicate::test),
            PublisherReducers.and());
   }

   @Override
   public OptionalInt findFirst() {
      // We aren't sorted, so just do findAny
      return findAny();
   }

   @Override
   public OptionalInt findAny() {
      Function<Publisher<Integer>, CompletionStage<Integer>> function = PublisherReducers.findFirst();
      Integer value = performPublisherOperation(function, function);
      if (value == null) {
         return OptionalInt.empty();
      }
      return OptionalInt.of(value);
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return remoteIterator();
   }

   PrimitiveIterator.OfInt remoteIterator() {
      // TODO: need to add in way to not box these later
      // Since this is a remote iterator we have to add it to the remote intermediate operations queue
      intermediateOperations.add(BoxedIntOperation.getInstance());
      DistributedCacheStream<Original, Integer> stream = new DistributedCacheStream<>(this);
      Iterator<Integer> iterator = stream.iterator();
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
      return performPublisherOperation(PublisherReducers.count(), PublisherReducers.add());
   }

   // These are the custom added methods for cache streams

   @Override
   public IntCacheStream sequentialDistribution() {
      parallelDistribution = false;
      return this;
   }

   @Override
   public IntCacheStream parallelDistribution() {
      parallelDistribution = true;
      return this;
   }

   @Override
   public IntCacheStream filterKeySegments(Set<Integer> segments) {
      return filterKeySegments(IntSets.from(segments));
   }

   @Override
   public IntCacheStream filterKeySegments(IntSet segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public IntCacheStream filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public IntCacheStream distributedBatchSize(int batchSize) {
      distributedBatchSize = batchSize;
      return this;
   }

   @Override
   public IntCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      if (segmentCompletionListener == null) {
         segmentCompletionListener = listener;
      } else {
         segmentCompletionListener = composeWithExceptions(segmentCompletionListener, listener);
      }
      return this;
   }

   @Override
   public IntCacheStream disableRehashAware() {
      rehashAware = false;
      return this;
   }

   @Override
   public IntCacheStream timeout(long timeout, TimeUnit unit) {
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

   protected DistributedDoubleCacheStream<Original> doubleCacheStream() {
      return new DistributedDoubleCacheStream<>(this);
   }

   protected DistributedLongCacheStream<Original> longCacheStream() {
      return new DistributedLongCacheStream<>(this);
   }
}
