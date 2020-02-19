package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.reactive.publisher.PublisherReducers;
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
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableBinaryOperator;
import org.infinispan.util.function.SerializableCallable;
import org.infinispan.util.function.SerializableComparator;
import org.infinispan.util.function.SerializableLongConsumer;
import org.infinispan.util.function.SerializableLongFunction;
import org.infinispan.util.function.SerializableLongPredicate;
import org.infinispan.util.function.SerializableLongToDoubleFunction;
import org.infinispan.util.function.SerializableLongToIntFunction;
import org.infinispan.util.function.SerializableLongUnaryOperator;
import org.infinispan.util.function.SerializableObjLongConsumer;
import org.infinispan.util.function.SerializablePredicate;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

/**
 * Implementation of {@link LongStream} that utilizes a lazily evaluated distributed back end execution.  Note this
 * class is only able to be created using {@link org.infinispan.CacheStream#mapToInt(ToIntFunction)} or similar
 * methods from the {@link org.infinispan.CacheStream} interface.
 * @param <Original> original stream type
 */
public class DistributedLongCacheStream<Original> extends AbstractCacheStream<Original, Long, LongStream, LongCacheStream>
        implements LongCacheStream {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   /**
    * This constructor is to be used only when a user calls a map or flat map method changing to an IntStream
    * from a CacheStream, Stream, DoubleStream, IntStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedLongCacheStream(AbstractCacheStream other) {
      super(other);
   }

   @Override
   protected Log getLog() {
      return log;
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
      // Distinct is applied remotely as well
      addIntermediateOperation(DistinctLongOperation.getInstance());
      return new IntermediateLongCacheStream(this).distinct();
   }

   @Override
   public LongCacheStream sorted() {
      return new IntermediateLongCacheStream(this).sorted();
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
      // Limit is applied remotely as well
      addIntermediateOperation(new LimitLongOperation(maxSize));
      return new IntermediateLongCacheStream(this).limit(maxSize);
   }

   @Override
   public LongCacheStream skip(long n) {
      return new IntermediateLongCacheStream(this).skip(n);
   }

   // Rest are terminal operators

   @Override
   public void forEach(LongConsumer action) {
      peek(action)
            .iterator()
            .forEachRemaining((long ignore) -> { });
   }

   @Override
   public void forEach(SerializableLongConsumer action) {
      forEach((LongConsumer) action);
   }

   @Override
   public <K, V> void forEach(ObjLongConsumer<Cache<K, V>> action) {
      peek(CacheBiConsumers.longConsumer(action))
            .iterator()
            .forEachRemaining((long ignore) -> { });
   }

   @Override
   public <K, V> void forEach(SerializableObjLongConsumer<Cache<K, V>> action) {
      forEach((ObjLongConsumer<Cache<K, V>>) action);
   }

   @Override
   public void forEachOrdered(LongConsumer action) {
      // Our stream is not sorted so just call forEach
      forEach(action);
   }

   @Override
   public long[] toArray() {
      Object[] values = performPublisherOperation(PublisherReducers.toArrayReducer(), PublisherReducers.toArrayFinalizer());

      long[] results = new long[values.length];
      int i = 0;
      for (Object obj : values) {
         results[i++] = (Long) obj;
      }
      return results;
   }

   @Override
   public long reduce(long identity, LongBinaryOperator op) {
      Function<Publisher<Long>, CompletionStage<Long>> reduce = PublisherReducers.reduce(identity,
            (SerializableBiFunction<Long, Long, Long>) op::applyAsLong);
      return performPublisherOperation(reduce, reduce);
   }

   @Override
   public OptionalLong reduce(LongBinaryOperator op) {
      Function<Publisher<Long>, CompletionStage<Long>> reduce = PublisherReducers.reduce(
            (SerializableBinaryOperator<Long>) op::applyAsLong);
      Long result = performPublisherOperation(reduce, reduce);
      if (result == null) {
         return OptionalLong.empty();
      }
      return OptionalLong.of(result);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return performPublisherOperation(PublisherReducers.collect(supplier,
            (SerializableBiConsumer<R, Long>) accumulator::accept),
            PublisherReducers.accumulate(combiner));
   }

   @Override
   public long sum() {
      Function<Publisher<Long>, CompletionStage<Long>> addFunction = PublisherReducers.add();
      return performPublisherOperation(addFunction, addFunction);
   }

   @Override
   public OptionalLong min() {
      SerializableComparator<Long> serializableComparator = Long::compareTo;
      Function<Publisher<Long>, CompletionStage<Long>> minFunction = PublisherReducers.min(serializableComparator);
      Long min = performPublisherOperation(minFunction, minFunction);
      if (min == null) {
         return OptionalLong.empty();
      }
      return OptionalLong.of(min);
   }

   @Override
   public OptionalLong max() {
      SerializableComparator<Long> serializableComparator = Long::compareTo;
      Function<Publisher<Long>, CompletionStage<Long>> maxFunction = PublisherReducers.max(serializableComparator);
      Long max = performPublisherOperation(maxFunction, maxFunction);
      if (max == null) {
         return OptionalLong.empty();
      }
      return OptionalLong.of(max);
   }

   @Override
   public OptionalDouble average() {
      LongSummaryStatistics lss = summaryStatistics();
      if (lss.getCount() == 0) {
         return OptionalDouble.empty();
      }
      return OptionalDouble.of(lss.getAverage());
   }

   @Override
   public LongSummaryStatistics summaryStatistics() {
      return performPublisherOperation(PublisherReducers.reduceWith(
            (SerializableCallable<LongSummaryStatistics>) LongSummaryStatistics::new,
            (SerializableBiFunction<LongSummaryStatistics, Long, LongSummaryStatistics>) (lss, longValue) -> {
               lss.accept(longValue);
               return lss;
            }), PublisherReducers.reduce(
            (SerializableBinaryOperator<LongSummaryStatistics>) (first, second) -> {
               first.combine(second);
               return first;
            }));
   }

   @Override
   public boolean anyMatch(LongPredicate predicate) {
      return performPublisherOperation(PublisherReducers.anyMatch((SerializablePredicate<Long>) predicate::test),
            PublisherReducers.or());
   }

   @Override
   public boolean allMatch(LongPredicate predicate) {
      return performPublisherOperation(PublisherReducers.allMatch((SerializablePredicate<Long>) predicate::test),
            PublisherReducers.and());
   }

   @Override
   public boolean noneMatch(LongPredicate predicate) {
      return performPublisherOperation(PublisherReducers.noneMatch((SerializablePredicate<Long>) predicate::test),
            PublisherReducers.and());
   }

   @Override
   public OptionalLong findFirst() {
      // Our stream is not sorted so just call findAny
      return findAny();
   }

   @Override
   public OptionalLong findAny() {
      Function<Publisher<Long>, CompletionStage<Long>> function = PublisherReducers.findFirst();
      Long value = performPublisherOperation(function, function);
      if (value == null) {
         return OptionalLong.empty();
      }
      return OptionalLong.of(value);
   }

   @Override
   public PrimitiveIterator.OfLong iterator() {
      return remoteIterator();
   }

   PrimitiveIterator.OfLong remoteIterator() {
      // TODO: need to add in way to not box these later
      // Since this is a remote iterator we have to add it to the remote intermediate operations queue
      intermediateOperations.add(BoxedLongOperation.getInstance());
      DistributedCacheStream<Original, Long> stream = new DistributedCacheStream<>(this);
      Iterator<Long> iterator = stream.iterator();
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
      return performPublisherOperation(PublisherReducers.count(), PublisherReducers.add());
   }

   // These are the custom added methods for cache streams

   @Override
   public LongCacheStream sequentialDistribution() {
      parallelDistribution = false;
      return this;
   }

   @Override
   public LongCacheStream parallelDistribution() {
      parallelDistribution = true;
      return this;
   }

   @Override
   public LongCacheStream filterKeySegments(Set<Integer> segments) {
      return filterKeySegments(IntSets.from(segments));
   }

   @Override
   public LongCacheStream filterKeySegments(IntSet segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public LongCacheStream filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public LongCacheStream distributedBatchSize(int batchSize) {
      distributedBatchSize = batchSize;
      return this;
   }

   @Override
   public LongCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      if (segmentCompletionListener == null) {
         segmentCompletionListener = listener;
      } else {
         segmentCompletionListener = composeWithExceptions(segmentCompletionListener, listener);
      }
      return this;
   }

   @Override
   public LongCacheStream disableRehashAware() {
      rehashAware = false;
      return this;
   }

   @Override
   public LongCacheStream timeout(long timeout, TimeUnit unit) {
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

   protected DistributedIntCacheStream<Original> intCacheStream() {
      return new DistributedIntCacheStream<>(this);
   }
}
