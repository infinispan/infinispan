package org.infinispan.stream.impl;

import org.infinispan.CacheStream;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.equivalence.EquivalentHashSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.termop.SegmentRetryingOperation;
import org.infinispan.stream.impl.termop.SingleRunOperation;
import org.infinispan.stream.impl.termop.object.FlatMapIteratorOperation;
import org.infinispan.stream.impl.termop.object.MapIteratorOperation;
import org.infinispan.stream.impl.termop.object.NoMapIteratorOperation;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstract stream that provides all of the common functionality required for all types of Streams including the various
 * primitive types.
 * @param <T> The type returned by the stream
 * @param <S> The stream interface
 */
public abstract class AbstractCacheStream<T, S extends BaseStream<T, S>, S2 extends S> implements BaseStream<T, S> {
   protected final Log log = LogFactory.getLog(getClass());

   protected final Queue<IntermediateOperation> intermediateOperations;
   protected Queue<IntermediateOperation> localIntermediateOperations;
   protected final Address localAddress;
   protected final DistributionManager dm;
   protected final Supplier<CacheStream<CacheEntry>> supplier;
   protected final ClusterStreamManager csm;
   protected final boolean includeLoader;
   protected final Executor executor;
   protected final ComponentRegistry registry;
   protected final PartitionHandlingManager partition;
   protected final Equivalence keyEquivalence;

   protected Runnable closeRunnable = null;

   protected boolean parallel;
   protected boolean sorted = false;
   protected boolean distinct = false;

   protected IntermediateType intermediateType = IntermediateType.NONE;

   protected Boolean parallelDistribution;
   protected boolean rehashAware = true;

   protected Set<?> keysToFilter;
   protected Set<Integer> segmentsToFilter;

   protected int distributedBatchSize;

   protected CacheStream.SegmentCompletionListener segmentCompletionListener;

   protected IteratorOperation iteratorOperation = IteratorOperation.NO_MAP;

   protected long timeout = 30;
   protected TimeUnit timeoutUnit = TimeUnit.SECONDS;

   protected AbstractCacheStream(Address localAddress, boolean parallel, DistributionManager dm,
           Supplier<CacheStream<CacheEntry>> supplier, ClusterStreamManager<Object> csm,
           boolean includeLoader, int distributedBatchSize, Executor executor, ComponentRegistry registry) {
      this.localAddress = localAddress;
      this.parallel = parallel;
      this.dm = dm;
      this.supplier = supplier;
      this.csm = csm;
      this.includeLoader = includeLoader;
      this.distributedBatchSize = distributedBatchSize;
      this.executor = executor;
      this.registry = registry;
      this.partition = registry.getComponent(PartitionHandlingManager.class);
      keyEquivalence = registry.getComponent(Configuration.class).dataContainer().keyEquivalence();
      intermediateOperations = new ArrayDeque<>();
   }

   protected AbstractCacheStream(AbstractCacheStream<T, S, S2> other) {
      this.intermediateOperations = other.intermediateOperations;
      this.localIntermediateOperations = other.localIntermediateOperations;
      this.localAddress = other.localAddress;
      this.dm = other.dm;
      this.supplier = other.supplier;
      this.csm = other.csm;
      this.includeLoader = other.includeLoader;
      this.executor = other.executor;
      this.registry = other.registry;
      this.partition = other.partition;
      this.keyEquivalence = other.keyEquivalence;

      this.closeRunnable = other.closeRunnable;

      this.parallel = other.parallel;
      this.sorted = other.sorted;
      this.distinct = other.distinct;

      this.intermediateType = other.intermediateType;

      this.parallelDistribution = other.parallelDistribution;
      this.rehashAware = other.rehashAware;

      this.keysToFilter = other.keysToFilter;
      this.segmentsToFilter = other.segmentsToFilter;

      this.distributedBatchSize = other.distributedBatchSize;

      this.segmentCompletionListener = other.segmentCompletionListener;

      this.iteratorOperation = other.iteratorOperation;

      this.timeout = other.timeout;
      this.timeoutUnit = other.timeoutUnit;
   }

   protected void markSorted(IntermediateType type) {
      if (intermediateType == IntermediateType.NONE) {
         intermediateType = type;
         if (localIntermediateOperations == null) {
            localIntermediateOperations = new ArrayDeque<>();
         }
      }
      sorted = true;
   }

   protected void markDistinct(IntermediateOperation<T, S, T, S> intermediateOperation, IntermediateType type) {
      intermediateOperation.handleInjection(registry);
      if (intermediateType == IntermediateType.NONE) {
         intermediateType = type;
         if (localIntermediateOperations == null) {
            localIntermediateOperations = new ArrayDeque<>();
            intermediateOperations.add(intermediateOperation);
         }
      }
      distinct = true;
   }

   protected void markSkip(IntermediateType type) {
      if (intermediateType == IntermediateType.NONE) {
         intermediateType = type;
         if (localIntermediateOperations == null) {
            localIntermediateOperations = new ArrayDeque<>();
         }
      }
      distinct = true;
   }

   protected S2 addIntermediateOperation(IntermediateOperation<T, S, T, S> intermediateOperation) {
      intermediateOperation.handleInjection(registry);
      if (localIntermediateOperations == null) {
         intermediateOperations.add(intermediateOperation);
      } else {
         localIntermediateOperations.add(intermediateOperation);
      }
      return unwrap();
   }

   protected void addIntermediateOperationMap(IntermediateOperation<T, S, ?, ?> intermediateOperation) {
      intermediateOperation.handleInjection(registry);
      if (localIntermediateOperations == null) {
         intermediateOperations.add(intermediateOperation);
      } else {
         localIntermediateOperations.add(intermediateOperation);
      }
   }

   protected abstract S2 unwrap();

   @Override
   public boolean isParallel() {
      return parallel;
   }

   boolean getParallelDistribution() {
      return parallelDistribution == null ? true : parallelDistribution;
   }

   @Override
   public S2 sequential() {
      parallel = false;
      return unwrap();
   }

   @Override
   public S2 parallel() {
      parallel = true;
      return unwrap();
   }

   @Override
   public S2 unordered() {
      sorted = false;
      return unwrap();
   }

   @Override
   public S2 onClose(Runnable closeHandler) {
      if (this.closeRunnable == null) {
         this.closeRunnable = closeHandler;
      } else {
         this.closeRunnable = composeWithExceptions(this.closeRunnable, closeHandler);
      }
      return unwrap();
   }

   @Override
   public void close() {
      if (closeRunnable != null) {
         closeRunnable.run();
      }
   }

   <R> R performOperation(Function<? super S2, ? extends R> function, boolean retryOnRehash, BinaryOperator<R> accumulator,
                          Predicate<? super R> earlyTerminatePredicate) {
      return performOperation(function, retryOnRehash, accumulator, earlyTerminatePredicate, true);
   }

   <R> R performOperation(Function<? super S2, ? extends R> function, boolean retryOnRehash, BinaryOperator<R> accumulator,
           Predicate<? super R> earlyTerminatePredicate, boolean ignoreSorting) {
      // These operations are not affected by sorting, only by distinct
      if (intermediateType.shouldUseIntermediate(!ignoreSorting && sorted, distinct)) {
         return performIntermediateRemoteOperation(function);
      } else {
         ResultsAccumulator<R> remoteResults = new ResultsAccumulator<>(accumulator);
         if (rehashAware) {
            return performOperationRehashAware(function, retryOnRehash, remoteResults, earlyTerminatePredicate);
         } else {
            return performOperation(function, remoteResults, earlyTerminatePredicate);
         }
      }
   }

   <R> R performOperation(Function<? super S2, ? extends R> function, ResultsAccumulator<R> remoteResults,
                          Predicate<? super R> earlyTerminatePredicate) {
      ConsistentHash ch = dm.getConsistentHash();
      TerminalOperation<R> op = new SingleRunOperation<>(intermediateOperations,
              supplierForSegments(ch, segmentsToFilter, null), function);
      Object id = csm.remoteStreamOperation(getParallelDistribution(), parallel, ch, segmentsToFilter, keysToFilter,
              Collections.emptyMap(), includeLoader, op, remoteResults, earlyTerminatePredicate);
      try {
         R localValue = op.performOperation();
         remoteResults.onCompletion(null, Collections.emptySet(), localValue);
         if (id != null) {
            try {
               if ((earlyTerminatePredicate == null || !earlyTerminatePredicate.test(localValue)) &&
                       !csm.awaitCompletion(id, timeout, timeoutUnit)) {
                  throw new TimeoutException();
               }
            } catch (InterruptedException e) {
               throw new CacheException(e);
            }
         }

         log.tracef("Finished operation for id %s", id);

         return remoteResults.currentValue;
      } finally {
         csm.forgetOperation(id);
      }
   }

   <R> R performOperationRehashAware(Function<? super S2, ? extends R> function, boolean retryOnRehash,
                                     ResultsAccumulator<R> remoteResults, Predicate<? super R> earlyTerminatePredicate) {
      Set<Integer> segmentsToProcess = segmentsToFilter;
      TerminalOperation<R> op;
      do {
         ConsistentHash ch = dm.getReadConsistentHash();
         if (retryOnRehash) {
            op = new SegmentRetryingOperation<>(intermediateOperations, supplierForSegments(ch, segmentsToProcess,
                    null), function);
         } else {
            op = new SingleRunOperation<>(intermediateOperations, supplierForSegments(ch, segmentsToProcess, null),
                    function);
         }
         Object id = csm.remoteStreamOperationRehashAware(getParallelDistribution(), parallel, ch, segmentsToProcess,
                 keysToFilter, Collections.emptyMap(), includeLoader, op, remoteResults, earlyTerminatePredicate);
         try {
            R localValue;
            boolean localRun = ch.getMembers().contains(localAddress);
            if (localRun) {
               localValue = op.performOperation();
               // TODO: we can do this more efficiently - since we drop all results locally
               if (dm.getReadConsistentHash().equals(ch)) {
                  Set<Integer> ourSegments = ch.getPrimarySegmentsForOwner(localAddress);
                  if (segmentsToProcess != null) {
                     ourSegments.retainAll(segmentsToProcess);
                  }
                  remoteResults.onCompletion(null, ourSegments, localValue);
               } else {
                  if (segmentsToProcess != null) {
                     Set<Integer> ourSegments = ch.getPrimarySegmentsForOwner(localAddress);
                     ourSegments.retainAll(segmentsToProcess);
                     remoteResults.onSegmentsLost(ourSegments);
                  } else {
                     remoteResults.onSegmentsLost(ch.getPrimarySegmentsForOwner(localAddress));
                  }
               }
            } else {
               // This isn't actually used because localRun short circuits first
               localValue = null;
            }
            if (id != null) {
               try {
                  if ((!localRun || earlyTerminatePredicate == null || !earlyTerminatePredicate.test(localValue)) &&
                          !csm.awaitCompletion(id, timeout, timeoutUnit)) {
                     throw new TimeoutException();
                  }
               } catch (InterruptedException e) {
                  throw new CacheException(e);
               }
            }

            if (!remoteResults.lostSegments.isEmpty()) {
               segmentsToProcess = new HashSet<>(remoteResults.lostSegments);
               remoteResults.lostSegments.clear();
               log.tracef("Found %s lost segments for identifier %s", segmentsToProcess, id);
            } else {
               // If we didn't lose any segments we don't need to process anymore
               if (segmentsToProcess != null) {
                  segmentsToProcess = null;
               }
               log.tracef("Finished rehash aware operation for id %s", id);
            }
         } finally {
            csm.forgetOperation(id);
         }
      } while (segmentsToProcess != null && !segmentsToProcess.isEmpty());

      return remoteResults.currentValue;
   }

   void performRehashKeyTrackingOperation(
           Function<Supplier<Stream<CacheEntry>>, KeyTrackingTerminalOperation<Object, ? extends T, Object>> function) {
      final AtomicBoolean complete = new AtomicBoolean();

      ConsistentHash segmentInfoCH = dm.getReadConsistentHash();
      KeyTrackingConsumer<Object, Object> results = new KeyTrackingConsumer<>(segmentInfoCH, (c) -> {},
              c -> c, null, keyEquivalence);
      Set<Integer> segmentsToProcess = segmentsToFilter == null ?
              new ReplicatedConsistentHash.RangeSet(segmentInfoCH.getNumSegments()) : segmentsToFilter;
      do {
         ConsistentHash ch = dm.getReadConsistentHash();
         boolean localRun = ch.getMembers().contains(localAddress);
         Set<Integer> segments;
         Set<Object> excludedKeys;
         if (localRun) {
            segments = ch.getPrimarySegmentsForOwner(localAddress);
            segments.retainAll(segmentsToProcess);

            excludedKeys = segments.stream().flatMap(s -> results.referenceArray.get(s).stream()).collect(
                    Collectors.toSet());
         } else {
            // This null is okay as it is only referenced if it was a localRun
            segments = null;
            excludedKeys = Collections.emptySet();
         }
         KeyTrackingTerminalOperation<Object, ? extends T, Object> op = function.apply(supplierForSegments(ch,
                 segmentsToProcess, excludedKeys));
         op.handleInjection(registry);
         Object id = csm.remoteStreamOperationRehashAware(getParallelDistribution(), parallel, ch, segmentsToProcess,
                 keysToFilter, new AtomicReferenceArrayToMap<>(results.referenceArray), includeLoader, op,
                 results);
         try {
            if (localRun) {
               Collection<CacheEntry<Object, Object>> localValue = op.performOperationRehashAware(results);
               // TODO: we can do this more efficiently - this hampers performance during rehash
               if (dm.getReadConsistentHash().equals(ch)) {
                  log.tracef("Found local values %s for id %s", localValue.size(), id);
                  results.onCompletion(null, segments, localValue);
               } else {
                  Set<Integer> ourSegments = ch.getPrimarySegmentsForOwner(localAddress);
                  ourSegments.retainAll(segmentsToProcess);
                  log.tracef("CH changed - making %s segments suspect for identifier %s", ourSegments, id);
                  results.onSegmentsLost(ourSegments);
                  // We keep track of those keys so we don't fire them again
                  results.onIntermediateResult(null, localValue);
               }
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
            if (!results.lostSegments.isEmpty()) {
               segmentsToProcess = new HashSet<>(results.lostSegments);
               results.lostSegments.clear();
               log.tracef("Found %s lost segments for identifier %s", segmentsToProcess, id);
            } else {
               log.tracef("Finished rehash aware operation for id %s", id);
               complete.set(true);
            }
         } finally {
            csm.forgetOperation(id);
         }
      } while (!complete.get());
   }

   static class AtomicReferenceArrayToMap<R> extends AbstractMap<Integer, R> {
      final AtomicReferenceArray<R> array;

      AtomicReferenceArrayToMap(AtomicReferenceArray<R> array) {
         this.array = array;
      }

      @Override
      public boolean containsKey(Object o) {
         if (!(o instanceof Integer))
            return false;
         int i = (int) o;
         return 0 <= i && i < array.length();
      }

      @Override
      public R get(Object key) {
         if (!(key instanceof Integer))
            return null;
         int i = (int) key;
         if (0 <= i && i < array.length()) {
            return array.get(i);
         }
         return null;
      }

      @Override
      public int size() {
         return array.length();
      }

      @Override
      public boolean remove(Object key, Object value) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Set<Entry<Integer, R>> entrySet() {
         // Do we want to implement this later?
         throw new UnsupportedOperationException();
      }
   }

   class KeyTrackingConsumer<K, V> implements ClusterStreamManager.ResultsCallback<Collection<CacheEntry<K, Object>>>,
           KeyTrackingTerminalOperation.IntermediateCollector<Collection<CacheEntry<K, Object>>> {
      final ConsistentHash ch;
      final Consumer<V> consumer;
      final Set<Integer> lostSegments = new ConcurrentHashSet<>();
      final Function<CacheEntry<K, Object>, V> valueFunction;

      final AtomicReferenceArray<Set<K>> referenceArray;

      final DistributedCacheStream.SegmentListenerNotifier listenerNotifier;

      KeyTrackingConsumer(ConsistentHash ch, Consumer<V> consumer, Function<CacheEntry<K, Object>, V> valueFunction,
              DistributedCacheStream.SegmentListenerNotifier completedSegments, Equivalence<? super K> keyEquivalence) {
         this.ch = ch;
         this.consumer = consumer;
         this.valueFunction = valueFunction;

         this.listenerNotifier = completedSegments;

         this.referenceArray = new AtomicReferenceArray<>(ch.getNumSegments());
         for (int i = 0; i < referenceArray.length(); ++i) {
            // We only allow 1 request per id
            referenceArray.set(i, new EquivalentHashSet<>(keyEquivalence));
         }
      }

      @Override
      public Set<Integer> onIntermediateResult(Address address, Collection<CacheEntry<K, Object>> results) {
         if (results != null) {
            log.tracef("Response from %s with results %s", address, results.size());
            Set<Integer> segmentsCompleted;
            CacheEntry<K, Object>[] lastCompleted = new CacheEntry[1];
            if (listenerNotifier != null) {
               segmentsCompleted = new HashSet<>();
            } else {
               segmentsCompleted = null;
            }
            results.forEach(e -> {
               K key = e.getKey();
               int segment = ch.getSegment(key);
               Set<K> keys = referenceArray.get(segment);
               // On completion we null this out first - thus we don't need to add
               if (keys != null) {
                  keys.add(key);
               } else if (segmentsCompleted != null) {
                  segmentsCompleted.add(segment);
                  lastCompleted[0] = e;
               }
               consumer.accept(valueFunction.apply(e));
            });
            if (lastCompleted[0] != null) {
               listenerNotifier.addSegmentsForObject(lastCompleted[0], segmentsCompleted);
               return segmentsCompleted;
            }
         }
         return null;
      }

      @Override
      public void onCompletion(Address address, Set<Integer> completedSegments, Collection<CacheEntry<K, Object>> results) {
         if (!completedSegments.isEmpty()) {
            log.tracef("Completing segments %s", completedSegments);
            // We null this out first so intermediate results don't add for no reason
            completedSegments.forEach(s -> referenceArray.set(s, null));
         } else {
            log.tracef("No segments to complete from %s", address);
         }
         Set<Integer> valueSegments = onIntermediateResult(address, results);
         if (valueSegments != null) {
            // We don't want to modify the completed segments as the caller may need it
            Set<Integer> emptyCompletedSegments = new HashSet<>(completedSegments.size());
            completedSegments.forEach(s -> {
               // First complete the segments that didn't have any keys - completed segments have to wait
               // until the user retrieves them
               if (!valueSegments.contains(s)) {
                  emptyCompletedSegments.add(s);
               }
            });
            listenerNotifier.completeSegmentsNoResults(emptyCompletedSegments);
         }
      }

      @Override
      public void onSegmentsLost(Set<Integer> segments) {
         // Have to use for loop since ConcurrentHashSet doesn't support addAll
         for (Integer segment : segments) {
            lostSegments.add(segment);
         }
      }

      @Override
      public void sendDataResonse(Collection<CacheEntry<K, Object>> response) {
         onIntermediateResult(null, response);
      }
   }

   static class ResultsAccumulator<R> implements ClusterStreamManager.ResultsCallback<R> {
      private final BinaryOperator<R> binaryOperator;
      private final Set<Integer> lostSegments = new ConcurrentHashSet<>();
      R currentValue;

      ResultsAccumulator(BinaryOperator<R> binaryOperator) {
         this.binaryOperator = binaryOperator;
      }

      @Override
      public Set<Integer> onIntermediateResult(Address address, R results) {
         if (results != null) {
            synchronized (this) {
               if (currentValue != null) {
                  currentValue = binaryOperator.apply(currentValue, results);
               } else {
                  currentValue = results;
               }
            }
         }
         return null;
      }

      @Override
      public void onCompletion(Address address, Set<Integer> completedSegments, R results) {
         onIntermediateResult(address, results);
      }

      @Override
      public void onSegmentsLost(Set<Integer> segments) {
         // Have to use for loop since ConcurrentHashSet doesn't support addAll
         for (Integer segment : segments) {
            lostSegments.add(segment);
         }
      }
   }

   static class CollectionConsumer<R> implements ClusterStreamManager.ResultsCallback<Collection<R>>,
           KeyTrackingTerminalOperation.IntermediateCollector<Collection<R>> {
      private final Consumer<R> consumer;

      CollectionConsumer(Consumer<R> consumer) {
         this.consumer = consumer;
      }

      @Override
      public Set<Integer> onIntermediateResult(Address address, Collection<R> results) {
         if (results != null) {
            results.forEach(consumer);
         }
         return null;
      }

      @Override
      public void onCompletion(Address address, Set<Integer> completedSegments, Collection<R> results) {
         onIntermediateResult(address, results);
      }

      @Override
      public void onSegmentsLost(Set<Integer> segments) {
      }

      @Override
      public void sendDataResonse(Collection<R> response) {
         onIntermediateResult(null, response);
      }
   }

   protected Supplier<Stream<CacheEntry>> supplierForSegments(ConsistentHash ch, Set<Integer> targetSegments,
                                                              Set<Object> excludedKeys) {
      return supplierForSegments(ch, targetSegments, excludedKeys, true);
   }

   /**
    * If <code>usePrimary</code> is true the segments are the primary segments but only those that exist in
    * targetSegments.  However if <code>usePrimary</code> is false then <code>targetSegments</code> must be
    * provided and non null and this will be used specifically.
    * @param ch
    * @param targetSegments
    * @param excludedKeys
    * @param usePrimary determines whether we should utilize the primary segments or not.
    * @return
    */
   protected Supplier<Stream<CacheEntry>> supplierForSegments(ConsistentHash ch, Set<Integer> targetSegments,
                                                              Set<Object> excludedKeys, boolean usePrimary) {
      if (!ch.getMembers().contains(localAddress)) {
         return Stream::empty;
      }
      Set<Integer> segments;
      if (usePrimary) {
         segments = ch.getPrimarySegmentsForOwner(localAddress);
         if (targetSegments != null) {
            segments.retainAll(targetSegments);
         }
      } else {
         segments = targetSegments;
      }

      return () -> {
         if (segments.isEmpty()) {
            return Stream.empty();
         }

         CacheStream<CacheEntry> stream = supplier.get().filterKeySegments(segments);
         if (keysToFilter != null) {
            stream = stream.filterKeys(keysToFilter);
         }
         if (excludedKeys != null) {
            return stream.filter(e -> !excludedKeys.contains(e.getKey()));
         }
         // Make sure the stream is set to be parallel or not
         return parallel ? stream.parallel() : stream.sequential();
      };
   }

   /**
    * Given two Runnables, return a Runnable that executes both in sequence,
    * even if the first throws an exception, and if both throw exceptions, add
    * any exceptions thrown by the second as suppressed exceptions of the first.
    */
   static Runnable composeWithExceptions(Runnable a, Runnable b) {
      return () -> {
         try {
            a.run();
         }
         catch (Throwable e1) {
            try {
               b.run();
            }
            catch (Throwable e2) {
               try {
                  e1.addSuppressed(e2);
               } catch (Throwable ignore) {}
            }
            throw e1;
         }
         b.run();
      };
   }

   enum IteratorOperation {
      NO_MAP {
         @Override
         public KeyTrackingTerminalOperation getOperation(Iterable<IntermediateOperation> intermediateOperations,
                                                          Supplier<Stream<CacheEntry>> supplier, int batchSize) {
            return new NoMapIteratorOperation<>(intermediateOperations, supplier, batchSize);
         }

         @Override
         public <K, V, R> Function<CacheEntry<K, V>, R> getFunction() {
            return e -> (R) e;
         }
      },
      MAP {
         @Override
         public KeyTrackingTerminalOperation getOperation(Iterable<IntermediateOperation> intermediateOperations,
                                                          Supplier<Stream<CacheEntry>> supplier, int batchSize) {
            return new MapIteratorOperation<>(intermediateOperations, supplier, batchSize);
         }
      },
      FLAT_MAP {
         @Override
         public KeyTrackingTerminalOperation getOperation(Iterable<IntermediateOperation> intermediateOperations,
                                                          Supplier<Stream<CacheEntry>> supplier, int batchSize) {
            return new FlatMapIteratorOperation<>(intermediateOperations, supplier, batchSize);
         }

         @Override
         public <V, V2> Consumer<V2> wrapConsumer(Consumer<V> consumer) {
            return new CollectionDecomposerConsumer(consumer);
         }
      };

      public abstract KeyTrackingTerminalOperation getOperation(Iterable<IntermediateOperation> intermediateOperations,
                                                       Supplier<Stream<CacheEntry>> supplier, int batchSize);

      public <K, V, R> Function<CacheEntry<K, V>, R> getFunction() {
         return e -> (R) e.getValue();
      }

      public <V, V2> Consumer<V2> wrapConsumer(Consumer<V> consumer) { return (Consumer<V2>) consumer; }
   }

   static class CollectionDecomposerConsumer<E> implements Consumer<Iterable<E>> {
      private final Consumer<E> consumer;

      CollectionDecomposerConsumer(Consumer<E> consumer) {
         this.consumer = consumer;
      }

      @Override
      public void accept(Iterable<E> es) {
         es.forEach(consumer);
      }
   }

   enum IntermediateType {
      OBJ,
      INT,
      DOUBLE,
      LONG,
      NONE {
         @Override
         public boolean shouldUseIntermediate(boolean sorted, boolean distinct) {
            return false;
         }
      };

      public boolean shouldUseIntermediate(boolean sorted, boolean distinct) {
         return sorted || distinct;
      }
   }

   <R> R performIntermediateRemoteOperation(Function<? super S2, ? extends R> function) {
      switch (intermediateType) {
         case OBJ:
            return performObjIntermediateRemoteOperation(function);
         case INT:
            return performIntegerIntermediateRemoteOperation(function);
         case DOUBLE:
            return performDoubleIntermediateRemoteOperation(function);
         case LONG:
            return performLongIntermediateRemoteOperation(function);
         default:
            throw new IllegalStateException("No intermediate state set");
      }
   }

   <R> R performIntegerIntermediateRemoteOperation(Function<? super S2, ? extends R> function) {
      // TODO: once we don't have to box for primitive iterators we can remove this copy
      Queue<IntermediateOperation> copyOperations = new ArrayDeque<>(localIntermediateOperations);
      PrimitiveIterator.OfInt iterator = new DistributedIntCacheStream(this).remoteIterator();
      SingleRunOperation<R, T, S, S2> op = new SingleRunOperation<>(copyOperations,
              () -> StreamSupport.intStream(Spliterators.spliteratorUnknownSize(
                      iterator, Spliterator.CONCURRENT), parallel), function);
      return op.performOperation();
   }

   <R> R performDoubleIntermediateRemoteOperation(Function<? super S2, ? extends R> function) {
      // TODO: once we don't have to box for primitive iterators we can remove this copy
      Queue<IntermediateOperation> copyOperations = new ArrayDeque<>(localIntermediateOperations);
      PrimitiveIterator.OfDouble iterator = new DistributedDoubleCacheStream(this).remoteIterator();
      SingleRunOperation<R, T, S, S2> op = new SingleRunOperation<>(copyOperations,
              () -> StreamSupport.doubleStream(Spliterators.spliteratorUnknownSize(
                      iterator, Spliterator.CONCURRENT), parallel), function);
      return op.performOperation();
   }

   <R> R performLongIntermediateRemoteOperation(Function<? super S2, ? extends R> function) {
      // TODO: once we don't have to box for primitive iterators we can remove this copy
      Queue<IntermediateOperation> copyOperations = new ArrayDeque<>(localIntermediateOperations);
      PrimitiveIterator.OfLong iterator = new DistributedLongCacheStream(this).remoteIterator();
      SingleRunOperation<R, T, S, S2> op = new SingleRunOperation<>(copyOperations,
              () -> StreamSupport.longStream(Spliterators.spliteratorUnknownSize(
                      iterator, Spliterator.CONCURRENT), parallel), function);
      return op.performOperation();
   }

   <R> R performObjIntermediateRemoteOperation(Function<? super S2, ? extends R> function) {
      Iterator<Object> iterator = new DistributedCacheStream<>(this).remoteIterator();
      SingleRunOperation<R, T, S, S2> op = new SingleRunOperation<>(localIntermediateOperations,
              () -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                      iterator, Spliterator.CONCURRENT), parallel), function);
      return op.performOperation();
   }
}
