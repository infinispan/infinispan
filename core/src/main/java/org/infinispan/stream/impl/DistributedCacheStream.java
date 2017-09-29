package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.CacheCollectors;
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
import org.infinispan.stream.impl.termop.object.NoMapIteratorOperation;
import org.infinispan.util.CloseableSuppliedIterator;
import org.infinispan.util.RangeSet;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.function.CloseableSupplier;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableBinaryOperator;
import org.infinispan.util.function.SerializableComparator;
import org.infinispan.util.function.SerializableConsumer;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.function.SerializableIntFunction;
import org.infinispan.util.function.SerializablePredicate;
import org.infinispan.util.function.SerializableSupplier;
import org.infinispan.util.function.SerializableToDoubleFunction;
import org.infinispan.util.function.SerializableToIntFunction;
import org.infinispan.util.function.SerializableToLongFunction;

import static org.infinispan.util.Casting.toSerialSupplierCollect;
import static org.infinispan.util.Casting.toSupplierCollect;

/**
 * Implementation of {@link CacheStream} that provides support for lazily distributing stream methods to appropriate
 * nodes
 * @param <R> The type of the stream
 */
public class DistributedCacheStream<R> extends AbstractCacheStream<R, Stream<R>, CacheStream<R>>
        implements CacheStream<R> {

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
   protected CacheStream<R> unwrap() {
      return this;
   }

   // Intermediate operations that are stored for lazy evalulation

   @Override
   public CacheStream<R> filter(Predicate<? super R> predicate) {
      return addIntermediateOperation(new FilterOperation<>(predicate));
   }

   @Override
   public CacheStream<R> filter(SerializablePredicate<? super R> predicate) {
      return filter((Predicate<? super R>) predicate);
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
   public <R1> CacheStream<R1> map(SerializableFunction<? super R, ? extends R1> mapper) {
      return map((Function<? super R, ? extends R1>) mapper);
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
   public IntCacheStream mapToInt(SerializableToIntFunction<? super R> mapper) {
      return mapToInt((ToIntFunction<? super R>) mapper);
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
   public LongCacheStream mapToLong(SerializableToLongFunction<? super R> mapper) {
      return mapToLong((ToLongFunction<? super R>) mapper);
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
   public DoubleCacheStream mapToDouble(SerializableToDoubleFunction<? super R> mapper) {
      return mapToDouble((ToDoubleFunction<? super R>) mapper);
   }

   @Override
   public <R1> CacheStream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      addIntermediateOperationMap(new FlatMapOperation<R, R1>(mapper));
      return (CacheStream<R1>) this;
   }

   @Override
   public <R1> CacheStream<R1> flatMap(SerializableFunction<? super R, ? extends Stream<? extends R1>> mapper) {
      return flatMap((Function<? super R, ? extends Stream<? extends R1>>) mapper);
   }

   @Override
   public IntCacheStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      addIntermediateOperationMap(new FlatMapToIntOperation<>(mapper));
      return intCacheStream();
   }

   @Override
   public IntCacheStream flatMapToInt(SerializableFunction<? super R, ? extends IntStream> mapper) {
      return flatMapToInt((Function<? super R, ? extends IntStream>) mapper);
   }

   @Override
   public LongCacheStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      addIntermediateOperationMap(new FlatMapToLongOperation<>(mapper));
      return longCacheStream();
   }

   @Override
   public LongCacheStream flatMapToLong(SerializableFunction<? super R, ? extends LongStream> mapper) {
      return flatMapToLong((Function<? super R, ? extends LongStream>) mapper);
   }

   @Override
   public DoubleCacheStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      addIntermediateOperationMap(new FlatMapToDoubleOperation<>(mapper));
      return doubleCacheStream();
   }

   @Override
   public DoubleCacheStream flatMapToDouble(SerializableFunction<? super R, ? extends DoubleStream> mapper) {
      return flatMapToDouble((Function<? super R, ? extends DoubleStream>) mapper);
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
   public CacheStream<R> sorted(SerializableComparator<? super R> comparator) {
      return sorted((Comparator<? super R>) comparator);
   }

   @Override
   public CacheStream<R> peek(Consumer<? super R> action) {
      return addIntermediateOperation(new PeekOperation<>(action));
   }

   @Override
   public CacheStream<R> peek(SerializableConsumer<? super R> action) {
      return peek((Consumer<? super R>) action);
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
   public R reduce(R identity, SerializableBinaryOperator<R> accumulator) {
      return reduce(identity, (BinaryOperator<R>) accumulator);
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
   public Optional<R> reduce(SerializableBinaryOperator<R> accumulator) {
      return reduce((BinaryOperator<R>) accumulator);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return performOperation(TerminalFunctions.reduceFunction(identity, accumulator, combiner), true, combiner, null);
   }

   @Override
   public <U> U reduce(U identity, SerializableBiFunction<U, ? super R, U> accumulator, SerializableBinaryOperator<U> combiner) {
      return reduce(identity, (BiFunction<U, ? super R, U>) accumulator, combiner);
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

   @Override
   public <R1> R1 collect(SerializableSupplier<R1> supplier, SerializableBiConsumer<R1, ? super R> accumulator,
           SerializableBiConsumer<R1, R1> combiner) {
      return collect((Supplier<R1>) supplier, accumulator, combiner);
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
   public <R1> R1 collect(SerializableSupplier<Collector<? super R, ?, R1>> supplier) {
      return collect(CacheCollectors.serializableCollector(toSerialSupplierCollect(supplier)));
   }

   @Override
   public <R1> R1 collect(Supplier<Collector<? super R, ?, R1>> supplier) {
      return collect(CacheCollectors.collector(toSupplierCollect(supplier)));
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
   public Optional<R> min(SerializableComparator<? super R> comparator) {
      return min((Comparator<? super R>) comparator);
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
   public Optional<R> max(SerializableComparator<? super R> comparator) {
      return max((Comparator<? super R>) comparator);
   }

   @Override
   public boolean anyMatch(Predicate<? super R> predicate) {
      return performOperation(TerminalFunctions.anyMatchFunction(predicate), false, Boolean::logicalOr, b -> b);
   }

   @Override
   public boolean anyMatch(SerializablePredicate<? super R> predicate) {
      return anyMatch((Predicate<? super R>) predicate);
   }

   @Override
   public boolean allMatch(Predicate<? super R> predicate) {
      return performOperation(TerminalFunctions.allMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean allMatch(SerializablePredicate<? super R> predicate) {
      return allMatch((Predicate<? super R>) predicate);
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return performOperation(TerminalFunctions.noneMatchFunction(predicate), false, Boolean::logicalAnd, b -> !b);
   }

   @Override
   public boolean noneMatch(SerializablePredicate<? super R> predicate) {
      return noneMatch((Predicate<? super R>) predicate);
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
      return remoteIterator();
   }

   Iterator<R> remoteIterator() {
      BlockingQueue<R> queue = new ArrayBlockingQueue<>(distributedBatchSize);

      final AtomicBoolean complete = new AtomicBoolean();

      Lock nextLock = new ReentrantLock();
      Condition nextCondition = nextLock.newCondition();

      Consumer<R> consumer = new HandOffConsumer<>(queue, complete, nextLock, nextCondition);

      IteratorSupplier<R> supplier = new IteratorSupplier<>(queue, complete, nextLock, nextCondition, csm);

      boolean iteratorParallelDistribute = parallelDistribution == null ? false : parallelDistribution;

      if (rehashAware) {
         rehashAwareIteration(complete, consumer, supplier, iteratorParallelDistribute);
      } else {
         ignoreRehashIteration(consumer, supplier, iteratorParallelDistribute);
      }

      CloseableIterator<R> closeableIterator = new CloseableSuppliedIterator<>(supplier);
      onClose(supplier::close);
      return closeableIterator;
   }

   private void ignoreRehashIteration(Consumer<R> consumer, IteratorSupplier<R> supplier, boolean iteratorParallelDistribute) {
      CollectionConsumer<R> remoteResults = new CollectionConsumer<>(consumer);
      ConsistentHash ch = dm.getWriteConsistentHash();

      boolean runLocal = ch.getMembers().contains(localAddress);
      boolean stayLocal = runLocal && segmentsToFilter != null
              && ch.getSegmentsForOwner(localAddress).containsAll(segmentsToFilter);

      NoMapIteratorOperation<?, R> op = new NoMapIteratorOperation<>(intermediateOperations, supplierForSegments(ch,
              segmentsToFilter, null, !stayLocal), distributedBatchSize);

      Thread thread = Thread.currentThread();
      executor.execute(() -> {
         try {
            log.tracef("Thread %s submitted iterator request for stream", thread);

            if (!stayLocal) {
               Object id = csm.remoteStreamOperation(iteratorParallelDistribute, parallel, ch, segmentsToFilter,
                       keysToFilter, Collections.<Object, Set<R>>emptyMap(), includeLoader, op, remoteResults);
               // Make sure to run this after we submit to the manager so it can process the other nodes
               // asynchronously with the local operation
               Collection<R> localValue = op.performOperation(remoteResults);
               remoteResults.onCompletion(null, Collections.emptySet(), localValue);
               if (id != null) {
                  supplier.pending = id;
                  try {
                     try {
                        if (!csm.awaitCompletion(id, timeout, timeoutUnit)) {
                           throw new TimeoutException();
                        }
                     } catch (InterruptedException e) {
                        throw new CacheException(e);
                     }

                  } finally {
                     csm.forgetOperation(id);
                  }
               }
            } else {
               Collection<R> localValue = op.performOperation(remoteResults);
               remoteResults.onCompletion(null, Collections.emptySet(), localValue);
            }
            supplier.close();
         } catch (CacheException e) {
            log.trace("Encountered local cache exception for stream", e);
            supplier.close(e);
         } catch (Throwable t) {
            log.trace("Encountered local throwable for stream", t);
            supplier.close(new CacheException(t));
         }
      });
   }

   private void rehashAwareIteration(AtomicBoolean complete, Consumer<R> consumer, IteratorSupplier<R> supplier, boolean iteratorParallelDistribute) {
      ConsistentHash segmentInfoCH = dm.getReadConsistentHash();
      SegmentListenerNotifier<R> listenerNotifier;
      if (segmentCompletionListener != null) {
         listenerNotifier = new SegmentListenerNotifier<>(
                 segmentCompletionListener);
         supplier.setConsumer(listenerNotifier);
      } else {
         listenerNotifier = null;
      }
      KeyTrackingConsumer<Object, R> results = new KeyTrackingConsumer<>(keyPartitioner,
            segmentInfoCH.getNumSegments(), iteratorOperation.wrapConsumer(consumer), iteratorOperation.getFunction(),
                                                                         listenerNotifier);
      Thread thread = Thread.currentThread();
      executor.execute(() -> {
         try {
            log.tracef("Thread %s submitted iterator request for stream", thread);
            Set<Integer> segmentsToProcess = segmentsToFilter == null ?
                                             new RangeSet(segmentInfoCH.getNumSegments()) : segmentsToFilter;
            do {
               LocalizedCacheTopology cacheTopology = dm.getCacheTopology();
               ConsistentHash ch = cacheTopology.getReadConsistentHash();
               boolean runLocal = ch.getMembers().contains(localAddress);
               Set<Integer> segments;
               Set<Object> excludedKeys;
               boolean stayLocal = false;
               if (runLocal) {
                  Set<Integer> segmentsForOwner = ch.getSegmentsForOwner(localAddress);
                  // If we own all of the segments locally, even as backup, we don't want the iterator to go remotely
                  stayLocal = segmentsForOwner.containsAll(segmentsToProcess);
                  if (stayLocal) {
                     segments = segmentsToProcess;
                  } else {
                     segments = ch.getPrimarySegmentsForOwner(localAddress).stream()
                             .filter(segmentsToProcess::contains).collect(Collectors.toSet());
                  }

                  excludedKeys = segments.stream().flatMap(s -> results.referenceArray.get(s).stream())
                          .collect(Collectors.toSet());
               } else {
                  segments = null;
                  excludedKeys = Collections.emptySet();
               }
               KeyTrackingTerminalOperation<Object, R, Object> op = iteratorOperation.getOperation(
                       intermediateOperations, supplierForSegments(ch, segmentsToProcess, excludedKeys, !stayLocal),
                       distributedBatchSize);
               if (!stayLocal) {
                  Object id = csm.remoteStreamOperationRehashAware(iteratorParallelDistribute, parallel, ch,
                          segmentsToProcess, keysToFilter, new AtomicReferenceArrayToMap<>(results.referenceArray),
                          includeLoader, op, results);
                  if (id != null) {
                     supplier.pending = id;
                  }
                  try {
                     if (runLocal) {
                        performLocalRehashAwareOperation(results, segmentsToProcess, ch, segments, op,
                                () -> ch.getPrimarySegmentsForOwner(localAddress), id);
                     }
                     if (id != null) {
                        try {
                           if (!csm.awaitCompletion(id, timeout, timeoutUnit)) {
                              throw new TimeoutException();
                           }
                        } catch (InterruptedException e) {
                           throw new CacheException(e);
                        }
                     }
                     segmentsToProcess = segmentsToProcess(supplier, results, segmentsToProcess, id, cacheTopology.getTopologyId());
                  } finally {
                     csm.forgetOperation(id);
                  }
               } else {
                  performLocalRehashAwareOperation(results, segmentsToProcess, ch, segments, op,
                          () -> ch.getSegmentsForOwner(localAddress), null);
                  segmentsToProcess = segmentsToProcess(supplier, results, segmentsToProcess, null, cacheTopology.getTopologyId());
               }
            } while (!complete.get());
         } catch (CacheException e) {
            log.trace("Encountered local cache exception for stream", e);
            supplier.close(e);
         } catch (Throwable t) {
            log.trace("Encountered local throwable for stream", t);
            supplier.close(new CacheException(t));
         }
      });
   }

   private Set<Integer> segmentsToProcess(IteratorSupplier<R> supplier, KeyTrackingConsumer<Object, R> results,
                                          Set<Integer> segmentsToProcess, Object id, int topologyId) {
      String strId = id == null ? "local" : id.toString();
      if (!results.lostSegments.isEmpty()) {
         segmentsToProcess = new HashSet<>(results.lostSegments);
         results.lostSegments.clear();
         log.tracef("Found %s lost segments for %s", segmentsToProcess, strId);
         if (results.requiresNextTopology) {
            try {
               stateTransferLock.waitForTopology(topologyId + 1, timeout, timeoutUnit);
            } catch (InterruptedException | java.util.concurrent.TimeoutException e) {
               throw new CacheException(e);
            }
         }
      } else {
         supplier.close();
         log.tracef("Finished rehash aware operation for %s", strId);
      }
      return segmentsToProcess;
   }

   private void performLocalRehashAwareOperation(KeyTrackingConsumer<Object, R> results,
                                                 Set<Integer> segmentsToProcess,
                                                 ConsistentHash ch,
                                                 Set<Integer> segments,
                                                 KeyTrackingTerminalOperation<Object, R, Object> op,
                                                 Supplier<Set<Integer>> ownedSegmentsSupplier,
                                                 Object id) {
      Collection<CacheEntry<Object, Object>> localValue = op.performOperationRehashAware(results);
      // TODO: we can do this more efficiently - this hampers performance during rehash
      if (dm.getReadConsistentHash().equals(ch)) {
         log.tracef("Found local values %s for id %s", localValue.size(), id);
         results.onCompletion(null, segments, localValue);
      } else {
         Set<Integer> ourSegments = ownedSegmentsSupplier.get();
         Set<Integer> lostSegments = ourSegments.stream().filter(segmentsToProcess::contains).collect(Collectors.toSet());
         log.tracef("CH changed - making %s segments suspect for identifier %s", lostSegments, id);
         results.onSegmentsLost(lostSegments);
      }
   }

   static class HandOffConsumer<R> implements Consumer<R> {
      private final BlockingQueue<R> queue;
      private final AtomicBoolean completed;
      private final Lock nextLock;
      private final Condition nextCondition;

      HandOffConsumer(BlockingQueue<R> queue, AtomicBoolean completed, Lock nextLock, Condition nextCondition) {
         this.queue = queue;
         this.completed = completed;
         this.nextLock = nextLock;
         this.nextCondition = nextCondition;
      }

      @Override
      public void accept(R rs) {
         // TODO: we don't awake people if they are waiting until we fill up the queue or process retrieves all values
         // is this the reason for slowdown?
         if (!queue.offer(rs)) {
            if (!completed.get()) {
               // Signal anyone waiting for values to consume from the queue
               nextLock.lock();
               try {
                  nextCondition.signalAll();
               } finally {
                  nextLock.unlock();
               }
               while (!completed.get()) {
                  // We keep trying to offer the value until it takes it.  In this case we check the completed after
                  // each time to make sure the iterator wasn't closed early
                  try {
                     if (queue.offer(rs, 100, TimeUnit.MILLISECONDS)) {
                        break;
                     }
                  } catch (InterruptedException e) {
                     throw new CacheException(e);
                  }
               }
            }
         }
      }
   }

   static class SegmentListenerNotifier<T> implements Consumer<T> {
      private final SegmentCompletionListener listener;
      // we know the objects will always be ==
      private final Map<T, Set<Integer>> segmentsByObject = new IdentityHashMap<>();

      SegmentListenerNotifier(SegmentCompletionListener listener) {
         this.listener = listener;
      }

      @Override
      public void accept(T t) {
         Set<Integer> segments = segmentsByObject.remove(t);
         if (segments != null) {
            listener.segmentCompleted(segments);
         }
      }

      public void addSegmentsForObject(T object, Set<Integer> segments) {
         segmentsByObject.put(object, segments);
      }

      public void completeSegmentsNoResults(Set<Integer> segments) {
         listener.segmentCompleted(segments);
      }
   }

   static class IteratorSupplier<R> implements CloseableSupplier<R> {
      private final BlockingQueue<R> queue;
      private final AtomicBoolean completed;
      private final Lock nextLock;
      private final Condition nextCondition;
      private final ClusterStreamManager<?> clusterStreamManager;

      CacheException exception;
      volatile Object pending;

      private Consumer<R> consumer;

      IteratorSupplier(BlockingQueue<R> queue, AtomicBoolean completed, Lock nextLock, Condition nextCondition,
              ClusterStreamManager<?> clusterStreamManager) {
         this.queue = queue;
         this.completed = completed;
         this.nextLock = nextLock;
         this.nextCondition = nextCondition;
         this.clusterStreamManager = clusterStreamManager;
      }

      @Override
      public void close() {
         close(null);
      }

      public void close(CacheException e) {
         nextLock.lock();
         try {
            if (!completed.getAndSet(true)) {
               if (e != null) {
                  exception = e;
               }
            }
            if (pending != null) {
               clusterStreamManager.forgetOperation(pending);
               pending = null;
            }
            nextCondition.signalAll();
         } finally {
            nextLock.unlock();
         }
      }

      @Override
      public R get() {
         R entry = queue.poll();
         if (entry == null) {
            if (completed.get()) {
               if (exception != null) {
                  throw exception;
               } else if ((entry = queue.poll()) != null) {
                  // We check the queue one last time to make sure we didn't have a concurrent queue addition and
                  // completed iterator
                  if (consumer != null) {
                     consumer.accept(entry);
                  }
                  return entry;
               }
               return null;
            }
            nextLock.lock();
            try {
               boolean interrupted = false;
               while (!completed.get()) {
                  // We should check to make sure nothing was added to the queue as well before sleeping
                  if ((entry = queue.poll()) != null) {
                     break;
                  }
                  try {
                     nextCondition.await(100, TimeUnit.MILLISECONDS);
                  } catch (InterruptedException e) {
                     // If interrupted, we just loop back around
                     interrupted = true;
                  }
               }
               if (entry == null) {
                  // If there is no entry and we are completed check one last time if there are entries in the queue
                  // It is possible for entries to be added to the queue and the iterator completed at the same time.
                  // Completed is a sign of either 3 things: all entries have been retrieved (what this case is for),
                  // an exception has been found in processing, or the user has manually closed the iterator.  In the
                  // latter 2 cases no additional entries are added to the queue since processing is stopped, therefore
                  // we can just process the rest of the elements in the queue with no worry.
                  entry = queue.poll();
                  if (entry == null) {
                     if (exception != null) {
                        throw exception;
                     }
                     return null;
                  }
               } else if (interrupted) {
                  // Now reset the interrupt state before returning
                  Thread.currentThread().interrupt();
               }
            } finally {
               nextLock.unlock();
            }
         }
         if (consumer != null) {
            consumer.accept(entry);
         }
         return entry;
      }

      public void setConsumer(Consumer<R> consumer) {
         this.consumer = consumer;
      }
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
   public void forEach(SerializableConsumer<? super R> action) {
      forEach((Consumer<? super R>) action);
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
   public <K, V> void forEach(SerializableBiConsumer<Cache<K, V>, ? super R> action) {
      forEach((BiConsumer<Cache<K, V>, ? super R>) action);
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

   @Override
   public <A> A[] toArray(SerializableIntFunction<A[]> generator) {
      return toArray((IntFunction<A[]>) generator);
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
   public CacheStream<R>
   filterKeySegments(Set<Integer> segments) {
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
