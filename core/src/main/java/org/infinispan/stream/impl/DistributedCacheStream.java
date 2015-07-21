package org.infinispan.stream.impl;

import org.infinispan.CacheStream;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.object.*;
import org.infinispan.stream.impl.termop.SingleRunOperation;
import org.infinispan.stream.impl.termop.object.ForEachOperation;
import org.infinispan.stream.impl.termop.object.NoMapIteratorOperation;
import org.infinispan.util.CloseableSuppliedIterator;
import org.infinispan.util.CloseableSupplier;
import org.infinispan.util.concurrent.TimeoutException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Implementation of {@link CacheStream} that provides support for lazily distributing stream methods to appropriate
 * nodes
 * @param <R> The type of the stream
 */
public class DistributedCacheStream<R> extends AbstractCacheStream<R, Stream<R>, Consumer<? super R>>
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
   protected Stream<R> unwrap() {
      return this;
   }

   // Intermediate operations that are stored for lazy evalulation

   @Override
   public Stream<R> filter(Predicate<? super R> predicate) {
      return addIntermediateOperation(new FilterOperation<>(predicate));
   }

   @Override
   public <R1> Stream<R1> map(Function<? super R, ? extends R1> mapper) {
      if (iteratorOperation != IteratorOperation.FLAT_MAP) {
         iteratorOperation = IteratorOperation.MAP;
      }
      return addIntermediateOperationMap(new MapOperation<>(mapper), (Stream<R1>) this);
   }

   @Override
   public IntStream mapToInt(ToIntFunction<? super R> mapper) {
      if (iteratorOperation != IteratorOperation.FLAT_MAP) {
         iteratorOperation = IteratorOperation.MAP;
      }
      return addIntermediateOperationMap(new MapToIntOperation<>(mapper), intCacheStream());
   }

   @Override
   public LongStream mapToLong(ToLongFunction<? super R> mapper) {
      if (iteratorOperation != IteratorOperation.FLAT_MAP) {
         iteratorOperation = IteratorOperation.MAP;
      }
      return addIntermediateOperationMap(new MapToLongOperation<>(mapper), longCacheStream());
   }

   @Override
   public DoubleStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      if (iteratorOperation != IteratorOperation.FLAT_MAP) {
         iteratorOperation = IteratorOperation.MAP;
      }
      return addIntermediateOperationMap(new MapToDoubleOperation<>(mapper), doubleCacheStream());
   }

   @Override
   public <R1> Stream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperationMap(new FlatMapOperation<>(mapper), (Stream<R1>) this);
   }

   @Override
   public IntStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperationMap(new FlatMapToIntOperation<>(mapper), intCacheStream());
   }

   @Override
   public LongStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperationMap(new FlatMapToLongOperation<>(mapper), longCacheStream());
   }

   @Override
   public DoubleStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      iteratorOperation = IteratorOperation.FLAT_MAP;
      return addIntermediateOperationMap(new FlatMapToDoubleOperation<>(mapper), doubleCacheStream());
   }

   @Override
   public Stream<R> distinct() {
      DistinctOperation op = DistinctOperation.getInstance();
      markDistinct(op, IntermediateType.OBJ);
      return addIntermediateOperation(op);
   }

   @Override
   public Stream<R> sorted() {
      markSorted(IntermediateType.OBJ);
      return addIntermediateOperation(SortedOperation.getInstance());
   }

   @Override
   public Stream<R> sorted(Comparator<? super R> comparator) {
      markSorted(IntermediateType.OBJ);
      return addIntermediateOperation(new SortedComparatorOperation<>(comparator));
   }

   @Override
   public Stream<R> peek(Consumer<? super R> action) {
      return addIntermediateOperation(new PeekOperation<>(action));
   }

   @Override
   public Stream<R> limit(long maxSize) {
      LimitOperation op = new LimitOperation<>(maxSize);
      markDistinct(op, IntermediateType.OBJ);
      return addIntermediateOperation(op);
   }

   @Override
   public Stream<R> skip(long n) {
      SkipOperation op = new SkipOperation<>(n);
      markSkip(IntermediateType.OBJ);
      return addIntermediateOperation(op);
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
      if (sorted) {
         // If we have a sorted map then we only have to worry about it being sorted if the collector is not
         // unordered
         sorted = !collector.characteristics().contains(Collector.Characteristics.UNORDERED);
      }
      // If it is not an identify finish we have to prevent the remote finisher, and apply locally only after
      // everything is combined.
      if (collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
         return performOperation(TerminalFunctions.collectorFunction(collector), true,
                 (BinaryOperator<R1>) collector.combiner(), null, false);
      } else {
         // Need to wrap collector to force identity finish
         A intermediateResult = performOperation(TerminalFunctions.collectorFunction(
                 new IdentifyFinishCollector<>(collector)), true, collector.combiner(), null, false);
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
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         Iterator<R> iterator = iterator();
         SingleRunOperation<Optional<R>, R, Stream<R>> op = new SingleRunOperation<>(localIntermediateOperations,
                 () -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                         iterator, Spliterator.CONCURRENT | Spliterator.NONNULL), parallel), s -> s.findFirst());
         return op.performOperation();
      } else {
         return findAny();
      }
   }

   @Override
   public Optional<R> findAny() {
      R value = performOperation(TerminalFunctions.findAnyFunction(), false, (r1, r2) -> r1 == null ? r2 : r1,
              a -> a != null);
      return Optional.ofNullable(value);
   }

   @Override
   public long count() {
      return performOperation(TerminalFunctions.countFunction(), true, (l1, l2) -> l1 + l2, null);
   }


   // The next ones are key tracking terminal operators

   @Override
   public Iterator<R> iterator() {
      if (intermediateType.shouldUseIntermediate(sorted, distinct)) {
         return performIntermediateRemoteOperation(s -> s.iterator());
      } else {
         return remoteIterator();
      }
   }

   Iterator<R> remoteIterator() {
      BlockingQueue<R> queue = new ArrayBlockingQueue<>(distributedBatchSize);

      final AtomicBoolean complete = new AtomicBoolean();

      Lock nextLock = new ReentrantLock();
      Condition nextCondition = nextLock.newCondition();

      Consumer<R> consumer = new HandOffConsumer(queue, complete, nextLock, nextCondition);

      IteratorSupplier<R> supplier = new IteratorSupplier(queue, complete, nextLock, nextCondition, csm);

      boolean iteratorParallelDistribute = parallelDistribution == null ? false : parallelDistribution;

      if (rehashAware) {
         ConsistentHash segmentInfoCH = dm.getReadConsistentHash();
         SegmentListenerNotifier<R> listenerNotifier;
         if (segmentCompletionListener != null) {
             listenerNotifier = new SegmentListenerNotifier<>(
                    segmentCompletionListener);
            supplier.setConsumer(listenerNotifier);
         } else {
            listenerNotifier = null;
         }
         KeyTrackingConsumer<Object, R> results = new KeyTrackingConsumer<>(segmentInfoCH,
                 iteratorOperation.wrapConsumer(consumer), iteratorOperation.getFunction(),
                 listenerNotifier);
         Thread thread = Thread.currentThread();
         executor.execute(() -> {
            try {
               log.tracef("Thread %s submitted iterator request for stream", thread);
               Set<Integer> segmentsToProcess = segmentsToFilter == null ?
                       new ReplicatedConsistentHash.RangeSet(segmentInfoCH.getNumSegments()) : segmentsToFilter;
               do {
                  ConsistentHash ch = dm.getReadConsistentHash();
                  boolean runLocal = ch.getMembers().contains(localAddress);
                  Set<Integer> segments;
                  Set<Object> excludedKeys;
                  if (runLocal) {
                     segments = ch.getPrimarySegmentsForOwner(localAddress);
                     segments.retainAll(segmentsToProcess);

                     excludedKeys = segments.stream().flatMap(s -> results.referenceArray.get(s).stream())
                             .collect(Collectors.toSet());
                  } else {
                     segments = null;
                     excludedKeys = Collections.emptySet();
                  }
                  KeyTrackingTerminalOperation<Object, R, Object> op = iteratorOperation.getOperation(
                          intermediateOperations, supplierForSegments(ch, segmentsToProcess, excludedKeys),
                          distributedBatchSize);
                  UUID id = csm.remoteStreamOperationRehashAware(iteratorParallelDistribute, parallel, ch,
                          segmentsToProcess, keysToFilter, new AtomicReferenceArrayToMap<>(results.referenceArray),
                          includeLoader, op, results);
                  supplier.pending = id;
                  try {
                     if (runLocal) {
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
                        }
                     }
                     try {
                        if (!csm.awaitCompletion(id, 30, TimeUnit.SECONDS)) {
                           throw new TimeoutException();
                        }
                     } catch (InterruptedException e) {
                        throw new CacheException(e);
                     }
                     if (!results.lostSegments.isEmpty()) {
                        segmentsToProcess = new HashSet<>(results.lostSegments);
                        results.lostSegments.clear();
                        log.tracef("Found %s lost segments for identifier %s", segmentsToProcess, id);
                     } else {
                        supplier.close();
                        log.tracef("Finished rehash aware operation for id %s", id);
                     }
                  } finally {
                     csm.forgetOperation(id);
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
      } else {
         CollectionConsumer<R> remoteResults = new CollectionConsumer<>(consumer);
         ConsistentHash ch = dm.getConsistentHash();
         NoMapIteratorOperation<?, R> op = new NoMapIteratorOperation<>(intermediateOperations, supplierForSegments(ch,
                 segmentsToFilter, null), distributedBatchSize);


         Thread thread = Thread.currentThread();
         executor.execute(() -> {
            try {
               log.tracef("Thread %s submitted iterator request for stream", thread);
               UUID id = csm.remoteStreamOperation(iteratorParallelDistribute, parallel, ch, segmentsToFilter,
                       keysToFilter, Collections.emptyMap(), includeLoader, op, remoteResults);
               supplier.pending = id;
               try {
                  Collection<R> localValue = op.performOperation(remoteResults);
                  remoteResults.onCompletion(null, Collections.emptySet(), localValue);
                  try {
                     if (!csm.awaitCompletion(id, 30, TimeUnit.SECONDS)) {
                        throw new TimeoutException();
                     }
                  } catch (InterruptedException e) {
                     throw new CacheException(e);
                  }

                  supplier.close();
               } finally {
                  csm.forgetOperation(id);
               }
            } catch (CacheException e) {
               log.trace("Encountered local cache exception for stream", e);
               supplier.close(e);
            } catch (Throwable t) {
               log.trace("Encountered local throwable for stream", t);
               supplier.close(new CacheException(t));
            }
         });
      }

      CloseableIterator<R> closeableIterator = new CloseableSuppliedIterator<>(supplier);
      onClose(() -> supplier.close());
      return closeableIterator;
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
      volatile UUID pending;

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
               }
               return null;
            }
            nextLock.lock();
            try {
               boolean interrupted = false;
               while ((entry = queue.poll()) == null && !completed.get()) {
                  try {
                     nextCondition.await(100, TimeUnit.MILLISECONDS);
                  } catch (InterruptedException e) {
                     // If interrupted, we just loop back around
                     interrupted = true;
                  }
               }
               if (entry == null) {
                  if (exception != null) {
                     throw exception;
                  }
                  return null;
               } else if (interrupted) {
                  // Now reset the interrupt state before returning
                  Thread.currentThread().interrupt();
               }
            } finally {
               nextLock.unlock();
            }
         }
         if (consumer != null && entry != null) {
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
         performRehashForEach(action);
      }
   }

   @Override
   KeyTrackingTerminalOperation getForEach(Consumer<? super R> consumer, Supplier<Stream<CacheEntry>> supplier) {
      return new ForEachOperation<>(intermediateOperations, supplier, distributedBatchSize, consumer);
   }

   @Override
   public void forEachOrdered(Consumer<? super R> action) {
      if (sorted) {
         Iterator<R> iterator = iterator();
         SingleRunOperation<Void, R,Stream<R>> op = new SingleRunOperation<>(localIntermediateOperations,
                 () -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                         iterator, Spliterator.CONCURRENT | Spliterator.NONNULL), parallel), s -> {
            s.forEachOrdered(action);
            return null;
         });
         op.performOperation();
      } else {
         forEach(action);
      }
   }

   @Override
   public Object[] toArray() {
      return performOperation(TerminalFunctions.toArrayFunction(), false,
              (v1, v2) -> {
                 Object[] array = Arrays.copyOf(v1, v1.length + v2.length);
                 System.arraycopy(v2, 0, array, v1.length, v2.length);
                 return array;
              }, null, false);
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      return performOperation(TerminalFunctions.toArrayFunction(generator), false,
              (v1, v2) -> {
                 A[] array = generator.apply(v1.length + v2.length);
                 System.arraycopy(v1, 0, array, 0, v1.length);
                 System.arraycopy(v2, 0, array, v1.length, v2.length);
                 return array;
              }, null, false);
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

   protected DistributedIntCacheStream intCacheStream() {
      return new DistributedIntCacheStream(this);
   }

   protected DistributedDoubleCacheStream doubleCacheStream() {
      return new DistributedDoubleCacheStream(this);
   }

   protected DistributedLongCacheStream longCacheStream() {
      return new DistributedLongCacheStream(this);
   }

   /**
    * Given two SegmentCompletionListener, return a SegmentCompletionListener that
    * executes both in sequence, even if the first throws an exception, and if both
    * throw exceptions, add any exceptions thrown by the second as suppressed
    * exceptions of the first.
    */
   protected static CacheStream.SegmentCompletionListener composeWithExceptions(CacheStream.SegmentCompletionListener a,
                                                                                CacheStream.SegmentCompletionListener b) {
      return (segments) -> {
         try {
            a.segmentCompleted(segments);
         }
         catch (Throwable e1) {
            try {
               b.segmentCompleted(segments);
            }
            catch (Throwable e2) {
               try {
                  e1.addSuppressed(e2);
               } catch (Throwable ignore) {}
            }
            throw e1;
         }
         b.segmentCompleted(segments);
      };
   }
}
