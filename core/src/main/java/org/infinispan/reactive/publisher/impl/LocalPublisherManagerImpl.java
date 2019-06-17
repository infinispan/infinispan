package org.infinispan.reactive.publisher.impl;

import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;

import java.lang.invoke.MethodHandles;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.rxjava.FlowableFromIntSetFunction;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.parallel.ParallelFlowable;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.UnicastProcessor;
import io.reactivex.schedulers.Schedulers;

/**
 * LocalPublisherManager that publishes entries from the local node only. This class handles suspecting segments
 * if they are lost while still processing them. The notification of the segments being lost is done by invoking
 * the {@link #segmentsLost(IntSet)} method.
 * @author wburns
 * @since 10.0
 */
@Scope(Scopes.NAMED_CACHE)
public class LocalPublisherManagerImpl<K, V> implements LocalPublisherManager<K, V> {
   private final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   @Inject ComponentRef<Cache<K, V>> cacheComponentRef;
   @Inject DistributionManager distributionManager;
   // This cache should only be used for retrieving entries via Cache#get
   protected AdvancedCache<K, V> remoteCache;
   // This cache should be used for iteration purposes or Cache#get that are local only
   protected AdvancedCache<K, V> cache;
   protected Scheduler asyncScheduler;
   protected int maxSegment;
   protected boolean hasLoader;
   protected final int cpuCount = ProcessorInfo.availableProcessors();

   protected final Set<SegmentListener> changeListener = ConcurrentHashMap.newKeySet();

   /**
    * Injects the cache - unfortunately this cannot be in start. Tests will rewire certain components which will in
    * turn reinject the cache, but they won't call the start method! If the latter is fixed we can add this to start
    * method and add @Inject to the variable.
    */
   @Inject
   public void inject(@ComponentName(ASYNC_OPERATIONS_EXECUTOR) ExecutorService asyncOperationsExecutor) {
      this.asyncScheduler = Schedulers.from(asyncOperationsExecutor);
   }

   @Start
   public void start() {
      // We need to unwrap the cache as a local stream should only deal with BOXED values
      // Any mappings will be provided by the originator node in their intermediate operation stack in the operation itself.
      this.remoteCache = AbstractDelegatingCache.unwrapCache(cacheComponentRef.running()).getAdvancedCache();
      // The iteration caches should only deal with local entries.
      // Also the iterations here are always remote initiated
      this.cache = remoteCache.withFlags(Flag.CACHE_MODE_LOCAL, Flag.REMOTE_ITERATION);
      hasLoader = cache.getCacheConfiguration().persistence().usingStores();
      ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
      this.maxSegment = clusteringConfiguration.hash().numSegments();
   }

   @Override
   public <R> CompletionStage<PublisherResult<R>> keyReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (keysToInclude != null) {
         return handleSpecificKeys(parallelPublisher, keysToInclude, keysToExclude, deliveryGuarantee, collator, finalizer);
      }

      AdvancedCache<K, V> cache = getCacheWithFlags(includeLoader);

      Function<K, K> toKeyFunction = Function.identity();
      switch (deliveryGuarantee) {
         case AT_MOST_ONCE:
            CompletionStage<R> stage = atMostOnce(parallelPublisher, cache.keySet(), keysToExclude, toKeyFunction,
                  segments, collator, finalizer);
            return stage.thenApply(ignoreSegmentsFunction());
         case AT_LEAST_ONCE:
            return atLeastOnce(parallelPublisher, cache.keySet(), keysToExclude, toKeyFunction, segments, collator, finalizer);
         case EXACTLY_ONCE:
            return exactlyOnce(parallelPublisher, cache.keySet(), keysToExclude, toKeyFunction, segments, collator, finalizer);
         default:
            throw new UnsupportedOperationException("Unsupported delivery guarantee: " + deliveryGuarantee);
      }
   }

   @Override
   public <R> CompletionStage<PublisherResult<R>> entryReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (keysToInclude != null) {
         return handleSpecificEntries(parallelPublisher, keysToInclude, keysToExclude, deliveryGuarantee, collator, finalizer);
      }

      AdvancedCache<K, V> cache = getCacheWithFlags(includeLoader);

      // We have to cast to Function, since we can't cast our inner generic
      Function<CacheEntry<K, V>, K> toKeyFunction = (Function) StreamMarshalling.entryToKeyFunction();
      switch (deliveryGuarantee) {
         case AT_MOST_ONCE:
            CompletionStage<R> stage = atMostOnce(parallelPublisher, cache.cacheEntrySet(), keysToExclude, toKeyFunction,
                  segments, collator, finalizer);
            return stage.thenApply(ignoreSegmentsFunction());
         case AT_LEAST_ONCE:
            return atLeastOnce(parallelPublisher, cache.cacheEntrySet(), keysToExclude, toKeyFunction, segments, collator, finalizer);
         case EXACTLY_ONCE:
            return exactlyOnce(parallelPublisher, cache.cacheEntrySet(), keysToExclude, toKeyFunction, segments, collator, finalizer);
         default:
            throw new UnsupportedOperationException("Unsupported delivery guarantee: " + deliveryGuarantee);
      }
   }

   @Override
   public void segmentsLost(IntSet lostSegments) {
      if (trace) {
         log.tracef("Notifying listeners of lost segments %s", lostSegments);
      }
      changeListener.forEach(lostSegments::forEach);
   }

   private static Function<Object, PublisherResult<Object>> ignoreSegmentsFunction  = value ->
         new SegmentPublisherResult<>(IntSets.immutableEmptySet(), value);

   static <R> Function<R, PublisherResult<R>> ignoreSegmentsFunction() {
      return (Function) ignoreSegmentsFunction;
   }

   private <I, R> void handleParallelSegment(PrimitiveIterator.OfInt segmentIter, int initialSegment, CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         FlowableProcessor<R> processor, IntSet concurrentSegments, SegmentListener listener) {
      // Indicates how many outstanding tasks we have. We can't complete the FlowableProcessor until
      // all are done. The invoking thread always has 1 for itself to ensure it is not completed early while
      // submitting the tasks. Note that this value is really only useful when a store is in use, as the returned
      // CompletionStage returned from the collator may not be completed (in memory it will always be complete).
      AtomicInteger pendingCompletions = new AtomicInteger(1);
      // This variable determines if our processor was serialized or not - We use a non serialized processor
      // for Publishers that are completed in the invoking thread. If a result was not completed immediately we have
      // to convert our Processor to be serialized to ensure we are only calling onNext from 1 thread at a time
      boolean serializedProcessor = false;
      try {
         while (true) {
            // The first run initialSegment will be 0 or greater. We use that segment and and set it to -1 to notify
            // our next run to try to steal a segment from the iterator to process until the iterator runs out
            // of segments
            int nextSegment;
            if (initialSegment != -1) {
               nextSegment = initialSegment;
               initialSegment = -1;
            } else {
               nextSegment = getNextSegment(segmentIter);
               if (nextSegment == -1) {
                  break;
               }
            }

            pendingCompletions.getAndIncrement();

            Flowable<I> innerFlowable = Flowable.fromPublisher(set.localPublisher(nextSegment));
            if (keysToExclude != null) {
               innerFlowable = innerFlowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
            }

            CompletionStage<R> stage = collator.apply(innerFlowable);
            // This will always be true if there isn't a store, however in most cases with a store this will
            // be false as we have to wait until the store can publish all the entries (which is done asynchronously)
            if (CompletionStages.isCompletedSuccessfully(stage)) {
               // If we complete the iteration try to remove the segment - so it can't be suspected
               concurrentSegments.remove(nextSegment);
               R notifiedValue;
               // This segment was lost before we could complete our iteration - so we have to discard the result
               if (listener.segmentsLost.contains(nextSegment)) {
                  notifiedValue = null;
               } else {
                  notifiedValue = CompletionStages.join(stage);

               }
               completeTask(pendingCompletions, notifiedValue, processor);
            } else {
               // If we have a stage that isn't complete we have to convert to a serialized processor as multiple
               // responses could come back at the same time
               if (!serializedProcessor) {
                  serializedProcessor = true;
                  processor = processor.toSerialized();
               }
               final FlowableProcessor<R> processorToUse = processor;
               stage.whenComplete((value, t) -> {
                  if (t != null) {
                     processorToUse.onError(t);
                  } else {
                     // If we complete the iteration try to remove the segment - so it can't be suspected
                     concurrentSegments.remove(nextSegment);
                     R notifiedValue;
                     // This segment was lost before we could complete our iteration - so we have to discard the result
                     if (listener.segmentsLost.contains(nextSegment)) {
                        notifiedValue = null;
                     } else {
                        notifiedValue = value;
                     }
                     completeTask(pendingCompletions, notifiedValue, processorToUse);
                  }
               });
            }
         }
         // Null value is ignored
         completeTask(pendingCompletions, null, processor);
      } catch (Throwable t) {
         processor.onError(t);
      }
   }

   private static <V> void completeTask(AtomicInteger count, V value, FlowableProcessor<V> processor) {
      if (value != null) {
         processor.onNext(value);
      }
      if (count.decrementAndGet() == 0) {
         processor.onComplete();
      }
   }

   /**
    * Retrieves the next int from the iterator in a thread safe manner. This method
    * synchronizes on the iterator instance, so be sure not to mix this object monitor with other invocations.
    * If the iterator has been depleted this method will return -1 instead.
    * @param segmentIter the iterator to retrieve the next segment from
    * @return the next segment or -1 if there are none left
    */
   private int getNextSegment(PrimitiveIterator.OfInt segmentIter) {
      synchronized (segmentIter) {
         if (segmentIter.hasNext()) {
            return segmentIter.nextInt();
         }
         return -1;
      }
   }

   private <I, R> CompletionStage<PublisherResult<R>> exactlyOnce(boolean parallelPublisher, CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      // This has to be concurrent to allow for different threads to update it (ie. parallel) or even ensure
      // that a state transfer segment lost can see completed
      IntSet concurrentSegments = IntSets.concurrentCopyFrom(segments, maxSegment);
      SegmentListener listener = new SegmentListener(concurrentSegments);
      changeListener.add(listener);

      listener.verifyTopology(distributionManager.getCacheTopology());

      Flowable<R> resultFlowable;
      if (parallelPublisher) {
         resultFlowable = exactlyOnceParallel(set, keysToExclude, toKeyFunction, segments, collator,
               listener, concurrentSegments);
      } else {
         resultFlowable = exactlyOnceSequential(set, keysToExclude, toKeyFunction, segments, collator,
               listener, concurrentSegments);
      }
      return exactlyOnceHandleLostSegments(finalizer.apply(resultFlowable), listener);
   }

   protected <R> CompletionStage<PublisherResult<R>> exactlyOnceHandleLostSegments(CompletionStage<R> finalValue, SegmentListener listener) {
      return handleLostSegments(finalValue, listener);
   }

   /**
    * This method iteratively submits a task to operate on the cpu bound thread pool up to the number of cores - 1.
    * The tasks perform a type of work stealing where they attempt to retrieve the next available segment and process
    * them as fast as possible. It is entirely possible that a given task is never submitted due to the other tasks
    * completing all the segments asynchronously. After the main thread has submitted all the tasks it will attempt
    * to steal a segment and run it if possible and if it can will subsequently attempt to complete all remaining
    * segments in the same fashion as the other threads. NOTE that this behavior is not normally how reactive
    * streams are done as given operations are not normally performed until the returned Flowable is subscribed to, but
    * for performance reasons this method eagerly publishes entries. This is because we do not have to context
    * switch an additional thread and we know that it is subscribed to immediately after.
    * <p>
    * The results of each segment data will then be published each as a single result in the returned Flowable. Due
    * to the results being retrieved eagerly it is entirely possible that if the Subscriber of the Flowable is slow
    * that that results queue up. But due to that the fact that results are reduced to single values for each segment
    * this shouldn't become an issue.
    * @param set CacheSet to retrieve the publisher for (non-nullable)
    * @param keysToExclude whether given keys should be excluded from the processing (nullable)
    * @param toKeyFunction function to convert an entry to a key to determine if it is excluded (must be non null if keysToExclude is)
    * @param segments the segments to process results for (non-nullable)
    * @param collator reducer to collate all the entries for a given segment into a single result (non-nullable)
    * @param listener listener that handles segments being lost and determining what results should be discarded (non-nullable)
    * @param concurrentSegments segments map of semgnets left to complete. remove an entry when a segment is completed to
    *                           prevent a data rehash causing a retry for the given segment
    * @param <I> input type of the data
    * @param <R> resulting value
    * @return Flowable that publishes a result for each segment
    */
   protected <I, R> Flowable<R> exactlyOnceParallel(CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         SegmentListener listener, IntSet concurrentSegments) {
      // The invoking thread will process entries so make sure we only have cpuCount number of tasks
      int extraThreadCount = cpuCount - 1;
      Flowable<R>[] processors = new Flowable[extraThreadCount + 1];
      PrimitiveIterator.OfInt segmentIter = segments.iterator();
      for (int i = 0; i < extraThreadCount; i++) {
         int initialSegment = getNextSegment(segmentIter);
         // If the iterator is already exhausted, don't submit to the remaining threads
         if (initialSegment == -1) {
            processors[i] = Flowable.empty();
            continue;
         }
         // This is specifically a UnicastProcessor as it allows for queueing of elements before you have subscribed
         // to the Flowable. It may be worth investigating using a PublishProcessor and eagerly subscribing to avoid
         // the cost of queueing the results.
         FlowableProcessor<R> processor = UnicastProcessor.create();
         processors[i] = processor;
         asyncScheduler.scheduleDirect(() ->
               handleParallelSegment(segmentIter, initialSegment, set, keysToExclude, toKeyFunction, collator, processor,
                     concurrentSegments, listener));
      }
      // After we have submitted all the tasks to other threads attempt to run the segments in our invoking thread
      int initialSegment = getNextSegment(segmentIter);
      if (initialSegment != -1) {
         FlowableProcessor<R> processor = UnicastProcessor.create();
         processors[extraThreadCount] = processor;
         handleParallelSegment(segmentIter, initialSegment, set, keysToExclude, toKeyFunction, collator, processor,
               concurrentSegments, listener);
      } else {
         processors[extraThreadCount] = Flowable.empty();
      }

      return ParallelFlowable.fromArray(processors).sequential();
   }

   protected <I, R> Flowable<R> exactlyOnceSequential(CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         SegmentListener listener, IntSet concurrentSegments) {
      return combineStages(new FlowableFromIntSetFunction<>(segments, segment -> {
         Flowable<I> innerFlowable = Flowable.fromPublisher(set.localPublisher(segment))
               // If we complete the iteration try to remove the segment - so it can't be suspected
               .doOnComplete(() -> concurrentSegments.remove(segment));

         if (keysToExclude != null) {
            innerFlowable = innerFlowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
         }

         CompletionStage<R> stage = collator.apply(innerFlowable);
         // This will always be true unless there is a store
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            if (listener.segmentsLost.contains(segment)) {
               return CompletableFutures.completedNull();
            }
            return stage;
         }

         return stage.thenCompose(value -> {
            // This means the segment was lost in the middle of processing
            if (listener.segmentsLost.contains(segment)) {
               return CompletableFutures.<R>completedNull();
            }
            return CompletableFuture.completedFuture(value);
         });
      }));
   }

   private <R> CompletionStage<PublisherResult<R>> handleSpecificKeys(boolean parallelPublisher, Set<K> keysToInclude,
         Set<K> keysToExclude, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      AdvancedCache<K, V> cache = deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE ? this.cache : remoteCache;
      return handleSpecificObjects(parallelPublisher, keysToInclude, keysToExclude, keyFlowable ->
            // Filter out all the keys that aren't in the cache
            keyFlowable.filter(cache::containsKey)
      , collator, finalizer);
   }

   private <R> CompletionStage<PublisherResult<R>> handleSpecificEntries(boolean parallelPublisher, Set<K> keysToInclude,
         Set<K> keysToExclude, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      AdvancedCache<K, V> cache = deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE ? this.cache : remoteCache;
      return handleSpecificObjects(parallelPublisher, keysToInclude, keysToExclude, keyFlowable ->
         keyFlowable.map(k -> {
            CacheEntry<K, V> entry = cache.getCacheEntry(k);
            if (entry == null) {
               return NullCacheEntry.<K, V>getInstance();
            }
            return entry;
         }).filter(e -> e != NullCacheEntry.getInstance())
      , collator, finalizer);
   }

   private <I, R> CompletionStage<PublisherResult<R>> handleSpecificObjects(boolean parallelPublisher, Set<K> keysToInclude,
         Set<K> keysToExclude, Function<? super Flowable<K>, ? extends Flowable<I>> keyTransformer,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      Flowable<K> keyFlowable = Flowable.fromIterable(keysToInclude);
      if (keysToExclude != null) {
         keyFlowable = keyFlowable.filter(k -> !keysToExclude.contains(k));
      }
      if (parallelPublisher) {
         // We send 16 keys to each rail to be parallelized - if ParallelFlowable had a method like railCompose
         // we could use it, but unfortunately it does not.
         Flowable<R> stageFlowable = keyFlowable.window(16)
               .flatMap(keys -> {
                  CompletionStage<R> stage = keyTransformer.apply(keys)
                        .subscribeOn(asyncScheduler)
                        .to(collator::apply);
                  return RxJavaInterop.<R>completionStageToPublisher().apply(stage);
               });
         return finalizer.apply(stageFlowable).thenApply(ignoreSegmentsFunction());
      } else {
         return keyTransformer.apply(keyFlowable)
               .to(collator::apply)
               .thenApply(ignoreSegmentsFunction());
      }
   }

   private <I, R> CompletionStage<R> parallelAtMostOnce(CacheSet<I> cacheSet, Set<K> keysToExclude,
         Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      Flowable<? extends CompletionStage<R>> stageFlowable = Flowable.fromIterable(segments)
            .parallel()
            .runOn(asyncScheduler)
            .map(segment -> {
               Flowable<I> innerFlowable = Flowable.fromPublisher(cacheSet.localPublisher(segment));
               if (keysToExclude != null) {
                  innerFlowable = innerFlowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
               }
               return collator.apply(innerFlowable);
            }).sequential();

      return combineStages(stageFlowable, finalizer);
   }

   private <I, R> CompletionStage<R> atMostOnce(boolean parallel, CacheSet<I> set, Set<K> keysToExclude,
         Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (parallel) {
         return parallelAtMostOnce(set, keysToExclude, toKeyFunction, segments, collator, finalizer);
      } else {
         Flowable<I> flowable = Flowable.fromPublisher(set.localPublisher(segments));
         if (keysToExclude != null) {
            flowable = flowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
         }
         return collator.apply(flowable);
      }
   }

   private <I, R> CompletionStage<PublisherResult<R>> atLeastOnce(boolean parallel, CacheSet<I> cacheSet,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      SegmentListener listener = new SegmentListener(segments);
      changeListener.add(listener);

      listener.verifyTopology(distributionManager.getCacheTopology());

      CompletionStage<R> stage = atMostOnce(parallel, cacheSet, keysToExclude, toKeyFunction, segments, collator, finalizer);
      return handleLostSegments(stage, listener);
   }

   protected <R> CompletionStage<PublisherResult<R>> handleLostSegments(CompletionStage<R> stage, SegmentListener segmentListener) {
      return stage.thenApply(value -> {
         IntSet lostSegments = segmentListener.segmentsLost;
         if (lostSegments.isEmpty()) {
            return LocalPublisherManagerImpl.<R>ignoreSegmentsFunction().apply(value);
         } else {
            return new SegmentPublisherResult<>(lostSegments, value);
         }
      }).whenComplete((u, t) -> changeListener.remove(segmentListener));
   }

   protected <R> CompletionStage<R> combineStages(Flowable<? extends CompletionStage<R>> stagePublisher,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return finalizer.apply(combineStages(stagePublisher));
   }

   protected <R> Flowable<R> combineStages(Flowable<? extends CompletionStage<R>> stagePublisher) {
      return stagePublisher.flatMap(stage -> {
         // We purposely send completedNull stage for when a segment is suspected
         if (stage == CompletableFutures.completedNull()) {
            return Flowable.empty();
         }

         if (CompletionStages.isCompletedSuccessfully(stage)) {
            R value = CompletionStages.join(stage);
            if (value == null) {
               return Flowable.empty();
            }
            return Flowable.just(value);
         }
         return RxJavaInterop.<R>completionStageToPublisher().apply(stage);
      });
   }

   private AdvancedCache<K, V> getCacheWithFlags(boolean includeLoader) {
      if (hasLoader && !includeLoader) {
         return cache.withFlags(Flag.SKIP_CACHE_LOAD);
      } else {
         return cache;
      }
   }

   protected class SegmentListener implements IntConsumer {
      protected final IntSet segments;
      protected final IntSet segmentsLost;

      SegmentListener(IntSet segments) {
         this.segments = segments;
         // This is a concurrent set for visibility and technically because state transfer could call this concurrently
         this.segmentsLost = IntSets.concurrentSet(maxSegment);
      }

      @Override
      public void accept(int segment) {
         if (segments.contains(segment)) {
            if (trace) {
               log.tracef("Listener %s lost segment %d", this, segment);
            }
            segmentsLost.set(segment);
         }
      }

      public void verifyTopology(LocalizedCacheTopology localizedCacheTopology) {
         for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
            int segment = segmentIterator.nextInt();
            if (!localizedCacheTopology.isSegmentReadOwner(segment)) {
               segmentsLost.set(segment);
            }
         }
      }
   }
}
