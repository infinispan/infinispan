package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.RangeSet;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.IntermediateOperation;
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
import org.infinispan.stream.impl.termop.object.ForEachBiOperation;
import org.infinispan.stream.impl.termop.object.ForEachOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * Implementation of {@link CacheStream} that provides support for lazily distributing stream methods to appropriate
 * nodes
 * @param <R> The type of the stream
 */
public class DistributedCacheStream<R> extends AbstractCacheStream<R, Stream<R>, CacheStream<R>>
        implements CacheStream<R> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   // This is a hack to allow for cast to work properly, since Java doesn't work as well with nested generics
   protected static Supplier<CacheStream<CacheEntry>> supplierStreamCast(Supplier supplier) {
      return supplier;
   }

   /**
    * Standard constructor requiring all pertinent information to properly utilize a distributed cache stream
    * @param localAddress the local address for this node
    * @param parallel whether or not this stream is parallel
    * @param dm the distribution manager to find out what keys map where
    * @param supplier a supplier of local cache stream instances.
    * @param csm manager that handles sending out messages to other nodes
    * @param includeLoader whether or not a cache loader should be utilized for these operations
    * @param distributedBatchSize default size of distributed batches
    * @param executor executor to be used for certain operations that require async processing (ie. iterator)
    */
   public <K, V> DistributedCacheStream(Address localAddress, boolean parallel, DistributionManager dm,
           Supplier<CacheStream<CacheEntry<K, V>>> supplier, ClusterStreamManager csm, boolean includeLoader,
           int distributedBatchSize, Executor executor, ComponentRegistry registry) {
      super(localAddress, parallel, dm, supplierStreamCast(supplier), csm, includeLoader, distributedBatchSize,
              executor, registry);
   }

   /**
    * Constructor that also allows a simple map method to be inserted first to change to another type.  This is
    * important because the {@link CacheStream#map(Function)} currently doesn't return a {@link CacheStream}.  If this
    * is changed we can remove this constructor and update references accordingly.
    * @param localAddress the local address for this node
    * @param parallel whether or not this stream is parallel
    * @param dm the distribution manager to find out what keys map where
    * @param supplier a supplier of local cache stream instances.
    * @param csm manager that handles sending out messages to other nodes
    * @param includeLoader whether or not a cache loader should be utilized for these operations
    * @param distributedBatchSize default size of distributed batches
    * @param executor executor to be used for certain operations that require async processing (ie. iterator)
    * @param function initial function to apply to the stream to change the type
    */
   public <K, V> DistributedCacheStream(Address localAddress, boolean parallel, DistributionManager dm,
           Supplier<CacheStream<CacheEntry<K, V>>> supplier, ClusterStreamManager csm, boolean includeLoader,
           int distributedBatchSize, Executor executor, ComponentRegistry registry,
           Function<? super CacheEntry<K, V>, R> function) {
      super(localAddress, parallel, dm, supplierStreamCast(supplier), csm, includeLoader, distributedBatchSize, executor,
              registry);
      intermediateOperations.add(new MapOperation(function));
      iteratorOperation = IteratorOperation.MAP;
   }

   /**
    * This constructor is to be used only when a user calls a map or flat map method changing back to a regular
    * Stream from an IntStream, DoubleStream etc.
    * @param other other instance of {@link AbstractCacheStream} to copy details from
    */
   protected DistributedCacheStream(AbstractCacheStream other) {
      super(other);
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
      return performOperation(TerminalFunctions.reduceFunction(identity, accumulator), true, accumulator, null);
   }

   @Override
   public Optional<R> reduce(BinaryOperator<R> accumulator) {
      R value = performOperation(TerminalFunctions.reduceFunction(accumulator), true,
              (e1, e2) -> {
                 if (e1 != null) {
                    if (e2 != null) {
                       return accumulator.apply(e1, e2);
                    }
                    return e1;
                 }
                 return e2;
              }, null);
      return Optional.ofNullable(value);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return performOperation(TerminalFunctions.reduceFunction(identity, accumulator, combiner), true, combiner, null);
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
      return performOperation(TerminalFunctions.collectFunction(supplier, accumulator, combiner), true,
              (e1, e2) -> {
                 combiner.accept(e1, e2);
                 return e1;
              }, null);
   }

   @SerializeWith(value = IdentifyFinishCollector.IdentityFinishCollectorExternalizer.class)
   private static final class IdentifyFinishCollector<T, A> implements Collector<T, A, A> {
      private final Collector<T, A, ?> realCollector;

      IdentifyFinishCollector(Collector<T, A, ?> realCollector) {
         this.realCollector = realCollector;
      }

      @Override
      public Supplier<A> supplier() {
         return realCollector.supplier();
      }

      @Override
      public BiConsumer<A, T> accumulator() {
         return realCollector.accumulator();
      }

      @Override
      public BinaryOperator<A> combiner() {
         return realCollector.combiner();
      }

      @Override
      public Function<A, A> finisher() {
         return null;
      }

      @Override
      public Set<Characteristics> characteristics() {
         Set<Characteristics> characteristics = realCollector.characteristics();
         if (characteristics.size() == 0) {
            return EnumSet.of(Characteristics.IDENTITY_FINISH);
         } else {
            Set<Characteristics> tweaked = EnumSet.copyOf(characteristics);
            tweaked.add(Characteristics.IDENTITY_FINISH);
            return tweaked;
         }
      }

      public static final class IdentityFinishCollectorExternalizer implements Externalizer<IdentifyFinishCollector> {
         @Override
         public void writeObject(ObjectOutput output, IdentifyFinishCollector object) throws IOException {
            output.writeObject(object.realCollector);
         }

         @Override
         public IdentifyFinishCollector readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new IdentifyFinishCollector((Collector) input.readObject());
         }
      }
   }

   @Override
   public <R1, A> R1 collect(Collector<? super R, A, R1> collector) {
      // If it is not an identify finish we have to prevent the remote finisher, and apply locally only after
      // everything is combined.
      if (collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
         return performOperation(TerminalFunctions.collectorFunction(collector), true,
                 (BinaryOperator<R1>) collector.combiner(), null);
      } else {
         // Need to wrap collector to force identity finish
         A intermediateResult = performOperation(TerminalFunctions.collectorFunction(
                 new IdentifyFinishCollector<>(collector)), true, collector.combiner(), null);
         return collector.finisher().apply(intermediateResult);
      }
   }

   @Override
   public Optional<R> min(Comparator<? super R> comparator) {
      R value = performOperation(TerminalFunctions.minFunction(comparator), false,
              (e1, e2) -> {
                 if (e1 != null) {
                    if (e2 != null) {
                       return comparator.compare(e1, e2) > 0 ? e2 : e1;
                    } else {
                       return e1;
                    }
                 }
                 return e2;
              }, null);
      return Optional.ofNullable(value);
   }

   @Override
   public Optional<R> max(Comparator<? super R> comparator) {
      R value = performOperation(TerminalFunctions.maxFunction(comparator), false,
              (e1, e2) -> {
                 if (e1 != null) {
                    if (e2 != null) {
                       return comparator.compare(e1, e2) > 0 ? e1 : e2;
                    } else {
                       return e1;
                    }
                 }
                 return e2;
              }, null);
      return Optional.ofNullable(value);
   }

   @Override
   public boolean anyMatch(Predicate<? super R> predicate) {
      return performOperation(TerminalFunctions.anyMatchFunction(predicate), false, Boolean::logicalOr, b -> b);
   }

   @Override
   public boolean allMatch(Predicate<? super R> predicate) {
      return performOperation(TerminalFunctions.allMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return performOperation(TerminalFunctions.noneMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public Optional<R> findFirst() {
      // We aren't sorted, so just do findAny
      return findAny();
   }

   @Override
   public Optional<R> findAny() {
      R value = performOperation(TerminalFunctions.findAnyFunction(), false, (r1, r2) -> r1 == null ? r2 : r1,
              Objects::nonNull);
      return Optional.ofNullable(value);
   }

   @Override
   public long count() {
      return performOperation(TerminalFunctions.countFunction(), true, (l1, l2) -> l1 + l2, null);
   }


   // The next ones are key tracking terminal operators

   @Override
   public Iterator<R> iterator() {
      log.tracef("Distributed iterator invoked with rehash: %s", rehashAware);
      if (!rehashAware) {
         // Non rehash doesn't care about lost segments or completed ones
         CloseableIterator<R> closeableIterator = nonRehashRemoteIterator(segmentsToFilter, null,
               IdentityPublisherDecorator.getInstance(), intermediateOperations);
         onClose(closeableIterator::close);
         return closeableIterator;
      } else {
         Iterable<IntermediateOperation> ops = iteratorOperation.prepareForIteration(intermediateOperations);
         CloseableIterator<R> closeableIterator;
         if (segmentCompletionListener != null && iteratorOperation != IteratorOperation.FLAT_MAP) {
            closeableIterator = new CompletionListenerRehashIterator<R>(ops, segmentCompletionListener);
         } else {
            closeableIterator = new RehashIterator<>(ops);
         }
         onClose(closeableIterator::close);
         // This cast messes up generic checking, but that is okay
         Function<R, R> function = iteratorOperation.getFunction();
         if (function != null) {
            return new IteratorMapper<>(closeableIterator, function);
         } else {
            return closeableIterator;
         }
      }
   }

   interface PublisherDecorator<S> {
      Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher);
      Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
            Publisher<S> localPublisher);
   }

   private static class IdentityPublisherDecorator<S, R> implements PublisherDecorator<S> {
      private static final IdentityPublisherDecorator decorator = new IdentityPublisherDecorator();
      private IdentityPublisherDecorator() { }

      static <S, R> IdentityPublisherDecorator<S, R> getInstance() {
         return decorator;
      }

      @Override
      public Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher) {
         return remotePublisher;
      }

      @Override
      public Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
            Publisher<S> localPublisher) {
         return localPublisher;
      }
   }

   private class RehashPublisherDecorator<S> implements PublisherDecorator<S> {
      private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments;
      private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments;
      private final Consumer<Object> keyConsumer;

      RehashPublisherDecorator(Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
            Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments, Consumer<Object> keyConsumer) {
         this.completedSegments = completedSegments;
         this.lostSegments = lostSegments;
         this.keyConsumer = keyConsumer;
      }

      @Override
      public Publisher<S> decorateRemote(ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher) {
         Publisher<S> convertedPublisher = s -> remotePublisher.subscribe(s, completedSegments, lostSegments);
         return decorateBeforeReturn(convertedPublisher);
      }

      @Override
      public Publisher<S> decorateLocal(ConsistentHash beginningCh, boolean onlyLocal, IntSet segmentsToFilter,
            Publisher<S> localPublisher) {
         Publisher<S> convertedPublisher = Flowable.fromPublisher(localPublisher).doOnComplete(() -> {
            IntSet ourSegments;
            if (onlyLocal) {
               ourSegments = SmallIntSet.from(beginningCh.getSegmentsForOwner(localAddress));
            } else {
               ourSegments = SmallIntSet.from(beginningCh.getPrimarySegmentsForOwner(localAddress));
            }
            ourSegments.retainAll(segmentsToFilter);
            // This will notify both completed and suspect of segments that may not even exist or were completed before
            // on a rehash
            if (dm.getReadConsistentHash().equals(beginningCh)) {
               log.tracef("Local iterator has completed segments %s", ourSegments);
               completedSegments.accept((Supplier<PrimitiveIterator.OfInt>) ourSegments::iterator);
            } else {
               log.tracef("Local iterator segments %s are all suspect as consistent hash has changed", ourSegments);
               lostSegments.accept((Supplier<PrimitiveIterator.OfInt>) ourSegments::iterator);
            }
         });
         return decorateBeforeReturn(convertedPublisher);
      }

      protected Publisher<S> decorateBeforeReturn(Publisher<S> publisher) {
         return iteratorOperation.handlePublisher(publisher, keyConsumer);
      }
   }

   private class RehashIterator<S> extends AbstractIterator<S> implements CloseableIterator<S> {
      private final AtomicReferenceArray<Set<Object>> receivedKeys;
      private final Iterable<IntermediateOperation> intermediateOperations;
      private final IntSet segmentsToUse;
      private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedHandler;

      private CloseableIterator<S> currentIterator;

      private RehashIterator(Iterable<IntermediateOperation> intermediateOperations) {
         this.intermediateOperations = intermediateOperations;
         int maxSegment = dm.getCacheTopology().getCurrentCH().getNumSegments();
         if (segmentsToFilter == null) {
            // We can't use RangeSet as we have to modify this IntSet
            segmentsToUse = new SmallIntSet(maxSegment);
            for (int i = 0; i < maxSegment; ++i) {
               segmentsToUse.set(i);
            }
         } else {
            // Need to make copy as we will modify this below
            segmentsToUse = new SmallIntSet(segmentsToFilter);
         }

         // TODO: we could optimize this array (make smaller) if we don't require all segments
         receivedKeys = new AtomicReferenceArray<>(maxSegment);
         for (int i = 0; i < receivedKeys.length(); ++i) {
            // Only 1 thread would be adding to a given set a time (a thread will only touch a subset of segments)
            receivedKeys.set(i, new HashSet<>());
         }

         completedHandler = completed -> {
            IntSet intSet;
            if (log.isTraceEnabled()) {
               intSet = new SmallIntSet();
            } else {
               intSet = null;
            }
            // For each completed segment we remove that segment and decrement our counter
            completed.get().forEachRemaining((int i) -> {
               // This way keys are able to be GC'd
               receivedKeys.set(i, null);
               if (intSet != null) {
                  intSet.set(i);
               }
               // in case if multiple responses occur (IntSet impl are not concurrent)
               synchronized (segmentsToUse) {
                  segmentsToUse.remove(i);
               }
            });

            if (intSet != null) {
               log.tracef("Remote rehash iterator completed segments %s", intSet);
            }
         };
      }

      @Override
      protected S getNext() {
         do {
            if (currentIterator == null) {
               log.tracef("Creating rehash iterator for segments %s", segmentsToUse);
               currentIterator = nonRehashRemoteIterator(segmentsToUse, receivedKeys::get,
                     publisherDecorator(completedHandler, lostSegments -> {
                     }, k -> {
                        // Every time a key is retrieved from iterator we add it to the keys received
                        // Then when we retry we exclude those keys to keep out duplicates
                        Set<Object> set = receivedKeys.get(keyPartitioner.getSegment(k));
                        if (set != null) {
                           set.add(k);
                        }
                     }), intermediateOperations);

            }
            if (currentIterator.hasNext()) {
               return currentIterator.next();
            } else {
               // Force new iterator once we have exhausted this one (if we have segments left)
               currentIterator = null;
            }
         } while (!segmentsToUse.isEmpty());

         return null;
      }

      PublisherDecorator<S> publisherDecorator(Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
            Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments, Consumer<Object> keyConsumer) {
         return new RehashPublisherDecorator<>(completedSegments, lostSegments, keyConsumer);
      }

      @Override
      public void close() {
         if (currentIterator != null) {
            currentIterator.close();
         }
      }
   }

   /**
    * Rehash Iterator which also has a completion listener. We cannot allow a segment to complete
    * until we are now releasing the last entry for a given set of segments
    * @param <S>
    */
   private class CompletionListenerRehashIterator<S> extends RehashIterator<S> {
      private final KeyWatchingCompletionListener completionListener;

      private CompletionListenerRehashIterator(Iterable<IntermediateOperation> intermediateOperations,
            Consumer<? super Supplier<PrimitiveIterator.OfInt>> completionListener) {
         super(intermediateOperations);
         this.completionListener = new KeyWatchingCompletionListener(completionListener);
      }

      @Override
      protected S getNext() {
         S next = super.getNext();
         if (next != null) {
            completionListener.valueIterated(next);
         } else {
            completionListener.completed();
         }
         return next;
      }

      @Override
      PublisherDecorator<S> publisherDecorator(Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
            Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments, Consumer<Object> keyConsumer) {
         Consumer<? super Supplier<PrimitiveIterator.OfInt>> ourCompleted = i -> {
            completionListener.segmentsEncountered(i);
            completedSegments.accept(i);
         };
         return new RehashPublisherDecorator<S>(ourCompleted, lostSegments, keyConsumer) {
            @Override
            protected Publisher<S> decorateBeforeReturn(Publisher<S> publisher) {
               return Flowable.fromPublisher(super.decorateBeforeReturn(publisher)).doOnNext(
                     completionListener::valueAdded);
            }
         };
      }
   }

   /**
    * Only notifies a completion listener when the last key for a segment has been found. The last key for a segment
    * is assumed to be the last key seen {@link KeyWatchingCompletionListener#valueAdded(Object)} before segments
    * are encountered {@link KeyWatchingCompletionListener#segmentsEncountered(Supplier)}.
    */
   private class KeyWatchingCompletionListener {
      private final AtomicReference<Object> currentKey = new AtomicReference<>();
      private final Map<Object, Supplier<PrimitiveIterator.OfInt>> pendingSegments = new ConcurrentHashMap<>();
      private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> completionListener;
      // The next 2 variables are possible assuming that the iterator is not used concurrently. This way we don't
      // have to allocate them on every entry iterated
      private final ByRef<Supplier<PrimitiveIterator.OfInt>> ref = new ByRef<>(null);
      private final BiFunction<Object, Supplier<PrimitiveIterator.OfInt>, Supplier<PrimitiveIterator.OfInt>> iteratorMapping;

      private KeyWatchingCompletionListener(Consumer<? super Supplier<PrimitiveIterator.OfInt>> completionListener) {
         this.completionListener = completionListener;
         this.iteratorMapping = (k, v) -> {
            if (v != null) {
               ref.set(v);
            }
            currentKey.compareAndSet(k, null);
            return null;
         };
      }

      public void valueAdded(Object key) {
         currentKey.set(key);
      }

      public void valueIterated(Object key) {
         pendingSegments.compute(key, iteratorMapping);
         if (ref.get() != null) {
            completionListener.accept(ref.get());
         }
      }

      private IntStream toStream(PrimitiveIterator.OfInt iter) {
         return StreamSupport.intStream(Spliterators.spliteratorUnknownSize(iter,
               Spliterator.DISTINCT | Spliterator.NONNULL), false);
      }

      public void segmentsEncountered(Supplier<PrimitiveIterator.OfInt> segments) {
         // This code assumes that valueAdded and segmentsEncountered are not invoked concurrently and that all values
         // added for a response before the segments are completed.
         // The valueIterated method can be invoked at any point however.
         // See ClusterStreamManagerImpl$ClusterStreamSubscription.sendRequest where the added and segments are called into
         AtomicBoolean shouldNotify = new AtomicBoolean(true);
         Object key = currentKey.get();
         if (key != null) {
            pendingSegments.compute(key, (k, v) -> {
               // This check is if iterator caught up to us
               if (currentKey.get() == null) {
                  return null;
               }
               shouldNotify.set(false);
               if (v == null) {
                  return segments;
               } else {
                  // This is ugly, but we shouldn't hit this often (need a response with no entries and completed
                  // segments and iterator that hasn't caught up)
                  return () -> Stream.of(segments, v).flatMapToInt(s -> toStream(s.get())).iterator();
               }
            });
         }
         if (shouldNotify.get()) {
            completionListener.accept(segments);
         }
      }

      public void completed() {
         pendingSegments.forEach((k, v) -> completionListener.accept(v));
      }
   }

   <S> Publisher<S> localPublisher(IntSet segmentsToFilter, ConsistentHash ch, Set<Object> excludedKeys,
         Iterable<IntermediateOperation> intermediateOperations, boolean stayLocal) {
      Supplier<Stream<CacheEntry>> supplier = supplierForSegments(ch, segmentsToFilter, excludedKeys, stayLocal);
      BaseStream stream = supplier.get();
      for (IntermediateOperation intermediateOperation : intermediateOperations) {
         stream = intermediateOperation.perform(stream);
      }
      BaseStream innerStream = stream;
      return Flowable.fromIterable(() -> innerStream.iterator());
   }

   <S> CloseableIterator<S> nonRehashRemoteIterator(IntSet segmentsToFilter, IntFunction<Set<Object>> keysToExclude,
         PublisherDecorator<S> publisherFunction, Iterable<IntermediateOperation> intermediateOperations) {
      ConsistentHash ch = dm.getReadConsistentHash();

      boolean stayLocal;

      Publisher<S> localPublisher;

      if (ch.getMembers().contains(localAddress)) {
         Set<Integer> ownedSegments = ch.getSegmentsForOwner(localAddress);
         if (segmentsToFilter == null) {
            stayLocal = ownedSegments.size() == ch.getNumSegments();
         } else {
            stayLocal = ownedSegments.containsAll(segmentsToFilter);
         }

         Publisher<S> innerPublisher = localPublisher(segmentsToFilter, ch,
               keysToExclude == null ? Collections.emptySet() :
                     (segmentsToFilter == null ? IntStream.range(0, ch.getNumSegments()) : segmentsToFilter.intStream())
                           .mapToObj(i -> keysToExclude.apply(i).stream()).flatMap(Function.identity()).collect(Collectors.toSet()),
               intermediateOperations, !stayLocal);

         localPublisher = publisherFunction.decorateLocal(ch, stayLocal, segmentsToFilter, innerPublisher);
      } else {
         stayLocal = false;
         localPublisher = Flowable.empty();
      }

      if (stayLocal) {
         return Closeables.iterator(Flowable.fromPublisher(localPublisher).blockingIterable().iterator());
      } else {
         Map<Address, IntSet> targets = determineTargets(ch, segmentsToFilter);
         Iterator<Map.Entry<Address, IntSet>> targetIter = targets.entrySet().iterator();

         // Parallel distribution is enabled by default, so it is only false if explicitly disabled
         if (parallelDistribution == Boolean.FALSE) {
            Supplier<Map.Entry<Address, IntSet>> supplier = () -> targetIter.hasNext() ? targetIter.next() : null;
            ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher = csm.remoteIterationPublisher(false,
                  supplier, keysToFilter, keysToExclude, includeLoader, intermediateOperations);
            Publisher<S> publisher = publisherFunction.decorateRemote(remotePublisher);

            // Local publisher is always last
            return PriorityMergingProcessor.build(publisher, distributedBatchSize, localPublisher, 64).iterator();
         } else {
            // Have to synchronize supplier retrieval as it could be called from 2 threads at once
            Supplier<Map.Entry<Address, IntSet>> supplier = () -> {
               synchronized (this) {
                  return targetIter.hasNext() ? targetIter.next() : null;
               }
            };
            PriorityMergingProcessor.Builder<S> builder = PriorityMergingProcessor.builder();
            // TODO: do we want to cap number of parallel distributions like this?
            for (int i = 0; i < 4 && i < targets.size(); ++i) {
               ClusterStreamManager.RemoteIteratorPublisher<S> remotePublisher = csm.remoteIterationPublisher(false,
                     supplier, keysToFilter, keysToExclude, includeLoader, intermediateOperations);
               Publisher<S> publisher = publisherFunction.decorateRemote(remotePublisher);

               builder.addPublisher(publisher, distributedBatchSize);
            }

            // Local publisher is always last
            return builder.addPublisher(localPublisher, 64).build().iterator();
         }
      }
   }

   private Map<Address, IntSet> determineTargets(ConsistentHash ch, IntSet segments) {
      if (segments == null) {
         segments = new RangeSet(ch.getNumSegments());
      }
      Map<Address, IntSet> targets = new HashMap<>();
      PrimitiveIterator.OfInt segmentIterator = segments.iterator();
      while (segmentIterator.hasNext()) {
         int segment = segmentIterator.nextInt();
         Address owner = ch.locatePrimaryOwnerForSegment(segment);
         if (owner == null || owner.equals(localAddress)) {
            continue;
         }
         IntSet targetSegments = targets.get(owner);
         if (targetSegments == null) {
            targetSegments = new SmallIntSet();
            targets.put(owner, targetSegments);
         }
         targetSegments.set(segment);
      }
      return targets;
   }

   @Override
   public Spliterator<R> spliterator() {
      return Spliterators.spliterator(iterator(), Long.MAX_VALUE, Spliterator.CONCURRENT);
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashKeyTrackingOperation(s -> new ForEachOperation<Object, R>(intermediateOperations, s, distributedBatchSize,
                 action));
      }
   }

   @Override
   public <K, V> void forEach(BiConsumer<Cache<K, V>, ? super R> action) {
      if (!rehashAware) {
         performOperation(TerminalFunctions.forEachFunction(action), false, (v1, v2) -> null, null);
      } else {
         performRehashKeyTrackingOperation(s -> new ForEachBiOperation(intermediateOperations, s,
                 distributedBatchSize, action));
      }
   }

   @Override
   public void forEachOrdered(Consumer<? super R> action) {
      // We aren't sorted, so just do forEach
      forEach(action);
   }

   @Override
   public Object[] toArray() {
      return performOperation(TerminalFunctions.toArrayFunction(), false,
              (v1, v2) -> {
                 Object[] array = Arrays.copyOf(v1, v1.length + v2.length);
                 System.arraycopy(v2, 0, array, v1.length, v2.length);
                 return array;
              }, null);
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      return performOperation(TerminalFunctions.toArrayFunction(generator), false,
              (v1, v2) -> {
                 A[] array = generator.apply(v1.length + v2.length);
                 System.arraycopy(v1, 0, array, 0, v1.length);
                 System.arraycopy(v2, 0, array, v1.length, v2.length);
                 return array;
              }, null);
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
      segmentsToFilter = SmallIntSet.from(segments);
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
