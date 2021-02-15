package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.SegmentCompletionPublisher;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.object.DistinctOperation;
import org.infinispan.stream.impl.intops.object.FilterOperation;
import org.infinispan.stream.impl.intops.object.FlatMapOperation;
import org.infinispan.stream.impl.intops.object.FlatMapToDoubleOperation;
import org.infinispan.stream.impl.intops.object.FlatMapToIntOperation;
import org.infinispan.stream.impl.intops.object.FlatMapToLongOperation;
import org.infinispan.stream.impl.intops.object.LimitOperation;
import org.infinispan.stream.impl.intops.object.MapOperation;
import org.infinispan.stream.impl.intops.object.MapToDoubleOperation;
import org.infinispan.stream.impl.intops.object.MapToIntOperation;
import org.infinispan.stream.impl.intops.object.MapToLongOperation;
import org.infinispan.stream.impl.intops.object.PeekOperation;
import org.infinispan.util.Closeables;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Implementation of {@link CacheStream} that provides support for lazily distributing stream methods to appropriate
 * nodes
 * @param <Original> the original type of the underlying stream - normally CacheEntry or Object
 * @param <R> The type of the stream
 */
public class DistributedCacheStream<Original, R> extends AbstractCacheStream<Original, R, Stream<R>, CacheStream<R>>
        implements CacheStream<R> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final int maxSegment;

   // This is a hack to allow for cast to work properly, since Java doesn't work as well with nested generics
   protected static <R> Supplier<CacheStream<R>> supplierStreamCast(Supplier supplier) {
      return supplier;
   }

   /**
    * Standard constructor requiring all pertinent information to properly utilize a distributed cache stream
    * @param localAddress the local address for this node
    * @param parallel whether or not this stream is parallel
    * @param dm the distribution manager to find out what keys map where
    * @param ctx the invocation context when this stream is created
    * @param supplier a supplier of local cache stream instances.
    * @param includeLoader whether or not a cache loader should be utilized for these operations
    * @param distributedBatchSize default size of distributed batches
    * @param executor executor to be used for certain operations that require async processing (ie. iterator)
    * @param registry component registry to wire objects with
    * @param toKeyFunction function that can be applied to an object in the stream to convert it to a key or null if it
    *                      is a key already. This variable is used to tell also if the underlying stream contains
    *                      entries or not by this value being non null
    */
   public DistributedCacheStream(Address localAddress, boolean parallel, DistributionManager dm, InvocationContext ctx,
           Supplier<CacheStream<R>> supplier, boolean includeLoader,
           int distributedBatchSize, Executor executor, ComponentRegistry registry, Function<? super Original, ?> toKeyFunction) {
      super(localAddress, parallel, dm, ctx, supplierStreamCast(supplier), includeLoader, distributedBatchSize,
              executor, registry, toKeyFunction);

      Configuration configuration = registry.getComponent(Configuration.class);
      maxSegment = configuration.clustering().hash().numSegments();
   }

   /**
    * This constructor is to be used only when a user calls a map or flat map method changing back to a regular
    * Stream from an IntStream, DoubleStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedCacheStream(AbstractCacheStream other) {
      super(other);

      Configuration configuration = registry.getComponent(Configuration.class);
      maxSegment = configuration.clustering().hash().numSegments();
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected CacheStream<R> unwrap() {
      return this;
   }

   // Intermediate operations that are stored for lazy evalulation

   @Override
   public CacheStream<R> filter(Predicate<? super R> predicate) {
      return addIntermediateOperation(new FilterOperation<>(predicate));
   }

   @Override
   public <R1> CacheStream<R1> map(Function<? super R, ? extends R1> mapper) {
      if (iteratorOperation != IteratorOperation.FLAT_MAP) {
         iteratorOperation = IteratorOperation.MAP;
      }
      addIntermediateOperationMap(new MapOperation<>(mapper));
      return (CacheStream<R1>) this;
   }

   @Override
   public IntCacheStream mapToInt(ToIntFunction<? super R> mapper) {
      if (iteratorOperation != IteratorOperation.FLAT_MAP) {
         iteratorOperation = IteratorOperation.MAP;
      }
      addIntermediateOperationMap(new MapToIntOperation<>(mapper));
      return intCacheStream();
   }

   @Override
   public LongCacheStream mapToLong(ToLongFunction<? super R> mapper) {
      if (iteratorOperation != IteratorOperation.FLAT_MAP) {
         iteratorOperation = IteratorOperation.MAP;
      }
      addIntermediateOperationMap(new MapToLongOperation<>(mapper));
      return longCacheStream();
   }

   @Override
   public DoubleCacheStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      if (iteratorOperation != IteratorOperation.FLAT_MAP) {
         iteratorOperation = IteratorOperation.MAP;
      }
      addIntermediateOperationMap(new MapToDoubleOperation<>(mapper));
      return doubleCacheStream();
   }

   @Override
   public <R1> CacheStream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      addIntermediateOperationMap(new FlatMapOperation<R, R1>(mapper));
      return (CacheStream<R1>) this;
   }

   @Override
   public IntCacheStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      addIntermediateOperationMap(new FlatMapToIntOperation<>(mapper));
      return intCacheStream();
   }

   @Override
   public LongCacheStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      addIntermediateOperationMap(new FlatMapToLongOperation<>(mapper));
      return longCacheStream();
   }

   @Override
   public DoubleCacheStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      addIntermediateOperationMap(new FlatMapToDoubleOperation<>(mapper));
      return doubleCacheStream();
   }

   @Override
   public CacheStream<R> distinct() {
      // Distinct is applied remotely as well
      addIntermediateOperation(DistinctOperation.getInstance());
      return new IntermediateCacheStream<>(this).distinct();
   }

   @Override
   public CacheStream<R> sorted() {
      return new IntermediateCacheStream<>(this).sorted();
   }

   @Override
   public CacheStream<R> sorted(Comparator<? super R> comparator) {
      return new IntermediateCacheStream<>(this).sorted(comparator);
   }

   @Override
   public CacheStream<R> peek(Consumer<? super R> action) {
      return addIntermediateOperation(new PeekOperation<>(action));
   }

   @Override
   public CacheStream<R> limit(long maxSize) {
      // Limit is applied remotely as well
      addIntermediateOperation(new LimitOperation<>(maxSize));
      return new IntermediateCacheStream<>(this).limit(maxSize);
   }

   @Override
   public CacheStream<R> skip(long n) {
      return new IntermediateCacheStream<>(this).skip(n);
   }

   // Now we have terminal operators

   @Override
   public R reduce(R identity, BinaryOperator<R> accumulator) {
      return performPublisherOperation(PublisherReducers.reduce(identity, accumulator),
            PublisherReducers.reduce(accumulator));
   }

   @Override
   public Optional<R> reduce(BinaryOperator<R> accumulator) {
      Function<Publisher<R>, CompletionStage<R>> function = PublisherReducers.reduce(accumulator);
      R value = performPublisherOperation(function, function);
      return Optional.ofNullable(value);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return performPublisherOperation(PublisherReducers.reduce(identity, accumulator),
            PublisherReducers.reduce(combiner));
   }

   /**
    * {@inheritDoc}
    * Note: this method doesn't pay attention to ordering constraints and any sorting performed on the stream will
    * be ignored by this terminal operator.  If you wish to have an ordered collector use the
    * {@link DistributedCacheStream#collect(Collector)} method making sure the
    * {@link java.util.stream.Collector.Characteristics#UNORDERED} property is not set.
    * @param supplier
    * @param accumulator
    * @param combiner
    * @param <R1>
    * @return
    */
   @Override
   public <R1> R1 collect(Supplier<R1> supplier, BiConsumer<R1, ? super R> accumulator, BiConsumer<R1, R1> combiner) {
      return performPublisherOperation(PublisherReducers.collect(supplier, accumulator),
            PublisherReducers.accumulate(combiner));
   }

   @Override
   public <R1, A> R1 collect(Collector<? super R, A, R1> collector) {
      A intermediateResult = performPublisherOperation(PublisherReducers.collectorReducer(collector),
            PublisherReducers.collectorFinalizer(collector));
      // Identify finish means we can just ignore the finisher method
      if (collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
         return (R1) intermediateResult;
      } else {
         return collector.finisher().apply(intermediateResult);
      }
   }

   @Override
   public Optional<R> min(Comparator<? super R> comparator) {
      Function<Publisher<R>, CompletionStage<R>> function = PublisherReducers.min(comparator);
      R value = performPublisherOperation(function, function);
      return Optional.ofNullable(value);
   }

   @Override
   public Optional<R> max(Comparator<? super R> comparator) {
      Function<Publisher<R>, CompletionStage<R>> function = PublisherReducers.max(comparator);
      R value = performPublisherOperation(function, function);
      return Optional.ofNullable(value);
   }

   @Override
   public boolean anyMatch(Predicate<? super R> predicate) {
      return performPublisherOperation(PublisherReducers.anyMatch(predicate), PublisherReducers.or());
   }

   @Override
   public boolean allMatch(Predicate<? super R> predicate) {
      return performPublisherOperation(PublisherReducers.allMatch(predicate), PublisherReducers.and());
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return performPublisherOperation(PublisherReducers.noneMatch(predicate), PublisherReducers.and());
   }

   @Override
   public Optional<R> findFirst() {
      // We aren't sorted, so just do findAny
      return findAny();
   }

   @Override
   public Optional<R> findAny() {
      Function<Publisher<R>, CompletionStage<R>> function = PublisherReducers.findFirst();
      R value = performPublisherOperation(function, function);
      return Optional.ofNullable(value);
   }

   @Override
   public long count() {
      return performPublisherOperation(PublisherReducers.count(), PublisherReducers.add());
   }


   // The next ones are key tracking terminal operators

   @Override
   public Iterator<R> iterator() {
      log.tracef("Distributed iterator invoked with rehash: %s", rehashAware);
      Function usedTransformer;
      if (intermediateOperations.isEmpty()) {
         usedTransformer = MarshallableFunctions.identity();
      } else {
         usedTransformer = new CacheIntermediatePublisher(intermediateOperations);
      }
      DeliveryGuarantee deliveryGuarantee = rehashAware ? DeliveryGuarantee.EXACTLY_ONCE : DeliveryGuarantee.AT_MOST_ONCE;
      Publisher<R> publisherToSubscribeTo;
      SegmentCompletionPublisher<R> publisher;
      if (toKeyFunction == null) {
         publisher = cpm.keyPublisher(segmentsToFilter, keysToFilter, invocationContext, includeLoader,
               deliveryGuarantee, distributedBatchSize, usedTransformer);
      } else {
         publisher = cpm.entryPublisher(segmentsToFilter, keysToFilter, invocationContext, includeLoader,
               deliveryGuarantee, distributedBatchSize, usedTransformer);
      }

      CompletionSegmentTracker segmentTracker;
      if (segmentCompletionListener != null) {
         // Tracker relies on ordering that a segment completion occurs
         segmentTracker = new CompletionSegmentTracker(segmentCompletionListener);
         publisherToSubscribeTo = Flowable.<R>fromPublisher(s -> publisher.subscribe(s, segmentTracker))
               .doOnNext(segmentTracker);
      } else {
         segmentTracker = null;
         publisherToSubscribeTo = publisher;
      }

      CloseableIterator<R> realIterator = Closeables.iterator(Flowable.fromPublisher(publisherToSubscribeTo)
            // Make sure any runtime errors are wrapped in CacheException
            .onErrorResumeNext(RxJavaInterop.cacheExceptionWrapper()), distributedBatchSize);
      onClose(realIterator::close);

      if (segmentTracker != null) {
         return new AbstractIterator<R>() {
            @Override
            protected R getNext() {
               if (realIterator.hasNext()) {
                  R value = realIterator.next();
                  segmentTracker.returningObject(value);
                  return value;
               } else {
                  segmentTracker.onComplete();
               }
               return null;
            }
         };
      }
      return realIterator;
   }

   /**
    * Tracking class that keeps track of segment completions and maps them to a given value. This value is not actually
    * part of these segments, but is instead the object returned immediately after the segments complete. This way
    * we can guarantee to notify the user after all elements have been processed of which segments were completed.
    * All methods except for accept(int) are guaranteed to be called sequentially and in a safe manner.
    */
   private class CompletionSegmentTracker implements IntConsumer, io.reactivex.rxjava3.functions.Consumer<Object> {
      private final Consumer<Supplier<PrimitiveIterator.OfInt>> listener;
      private final Map<Object, IntSet> awaitingNotification;
      volatile IntSet completedSegments;

      private CompletionSegmentTracker(Consumer<Supplier<PrimitiveIterator.OfInt>> listener) {
         this.listener = Objects.requireNonNull(listener);
         this.awaitingNotification = new HashMap<>();
         this.completedSegments = IntSets.concurrentSet(maxSegment);
      }


      @Override
      public void accept(int value) {
         // This method can technically be called from multiple threads
         completedSegments.set(value);
      }

      @Override
      public void accept(Object r) {
         if (!completedSegments.isEmpty()) {
            log.tracef("Going to complete segments %s when %s is iterated upon", completedSegments, Util.toStr(r));
            awaitingNotification.put(r, completedSegments);
            completedSegments = IntSets.concurrentSet(maxSegment);
         }
      }

      public void returningObject(Object value) {
         IntSet segments = awaitingNotification.remove(value);
         if (segments != null) {
            log.tracef("Notifying listeners of segments %s complete now that %s is returning", segments, Util.toStr(value));
            listener.accept(segments::iterator);
         }
      }

      public void onComplete() {
         log.tracef("Completing last segments of: %s", completedSegments);
         listener.accept(completedSegments::iterator);
         completedSegments.clear();
      }
   }

   @Override
   public Spliterator<R> spliterator() {
      return Spliterators.spliterator(iterator(), Long.MAX_VALUE, Spliterator.CONCURRENT);
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      peek(action)
            .iterator()
            .forEachRemaining(ignore -> { });
   }

   @Override
   public <K, V> void forEach(BiConsumer<Cache<K, V>, ? super R> action) {
      peek(CacheBiConsumers.objectConsumer(action))
            .iterator()
            .forEachRemaining(ignore -> { });
   }

   @Override
   public void forEachOrdered(Consumer<? super R> action) {
      // We aren't sorted, so just do forEach
      forEach(action);
   }

   @Override
   public Object[] toArray() {
      return performPublisherOperation(PublisherReducers.toArrayReducer(), PublisherReducers.toArrayFinalizer());
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      // The types are really Function<Publisher<R>, CompletionStage<A[]>> but to help users call toArrayReducer with
      // proper compile type checks it forces a type restriction that the generated array must be a super class
      // of the stream type. Unfortunately Stream API does not have that restriction and thus only throws
      // a RuntimeException instead.
      Function function = PublisherReducers.toArrayReducer(generator);
      return (A[]) performPublisherOperation(function, PublisherReducers.toArrayFinalizer(generator));
   }

   // These are the custom added methods for cache streams

   @Override
   public CacheStream<R> sequentialDistribution() {
      parallelDistribution = false;
      return this;
   }

   @Override
   public CacheStream<R> parallelDistribution() {
      parallelDistribution = true;
      return this;
   }

   @Override
   public CacheStream<R> filterKeySegments(Set<Integer> segments) {
      segmentsToFilter = IntSets.from(segments);
      return this;
   }

   @Override
   public CacheStream<R> filterKeySegments(IntSet segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public CacheStream<R> filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public CacheStream<R> distributedBatchSize(int batchSize) {
      distributedBatchSize = batchSize;
      return this;
   }

   @Override
   public CacheStream<R> segmentCompletionListener(SegmentCompletionListener listener) {
      if (segmentCompletionListener == null) {
         segmentCompletionListener = listener;
      } else {
         segmentCompletionListener = composeWithExceptions(segmentCompletionListener, listener);
      }
      return this;
   }

   @Override
   public CacheStream<R> disableRehashAware() {
      rehashAware = false;
      return this;
   }

   @Override
   public CacheStream<R> timeout(long timeout, TimeUnit unit) {
      if (timeout <= 0) {
         throw new IllegalArgumentException("Timeout must be greater than 0");
      }
      this.timeout = timeout;
      this.timeoutUnit = unit;
      return this;
   }

   protected DistributedIntCacheStream intCacheStream() {
      return new DistributedIntCacheStream(this);
   }

   protected DistributedDoubleCacheStream doubleCacheStream() {
      return new DistributedDoubleCacheStream(this);
   }

   protected DistributedLongCacheStream longCacheStream() {
      return new DistributedLongCacheStream(this);
   }
}
