package org.infinispan.reactive.publisher.impl;

import static org.infinispan.context.InvocationContextFactory.UNBOUNDED;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;

import java.lang.invoke.MethodHandles;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager.StoreChangeListener;
import org.infinispan.reactive.publisher.impl.commands.reduction.PublisherResult;
import org.infinispan.reactive.publisher.impl.commands.reduction.SegmentPublisherResult;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableConverter;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.functions.Predicate;
import io.reactivex.rxjava3.parallel.ParallelFlowable;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.schedulers.Schedulers;

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
   static final int PARALLEL_BATCH_SIZE = 1024;

   @Inject ComponentRef<Cache<K, V>> cacheComponentRef;
   @Inject DistributionManager distributionManager;
   @Inject PersistenceManager persistenceManager;
   @Inject Configuration configuration;
   @Inject KeyPartitioner keyPartitioner;
   @Inject ComponentRef<InvocationHelper> invocationHelper;
   @Inject CommandsFactory commandsFactory;
   @Inject InvocationContextFactory invocationContextFactory;
   // This cache should only be used for retrieving entries via Cache#get
   protected AdvancedCache<K, V> remoteCache;
   // This cache should be used for iteration purposes or Cache#get that are local only
   protected AdvancedCache<K, V> cache;
   protected Scheduler nonBlockingScheduler;
   protected int maxSegment;
   protected final int cpuCount = ProcessorInfo.availableProcessors();

   protected final Set<IntConsumer> changeListener = ConcurrentHashMap.newKeySet();

   private final LocalEntryPublisherStrategy nonSegmentedPublisher = new NonSegmentedEntryPublisherStrategy();
   private final LocalEntryPublisherStrategy segmentedPublisher = new SegmentedLocalPublisherStrategyLocal();

   private volatile LocalEntryPublisherStrategy localPublisherStrategy;
   private final StoreChangeListener storeChangeListener = pm -> updateStrategy(pm.usingSegmentedStore());

   /**
    * Injects the cache - unfortunately this cannot be in start. Tests will rewire certain components which will in
    * turn reinject the cache, but they won't call the start method! If the latter is fixed we can add this to start
    * method and add @Inject to the variable.
    */
   @Inject
   public void inject(@ComponentName(NON_BLOCKING_EXECUTOR) ExecutorService nonBlockingExecutor) {
      this.nonBlockingScheduler = Schedulers.from(nonBlockingExecutor);
   }

   @Start
   public void start() {
      // We need to unwrap the cache as a local stream should only deal with BOXED values
      // Any mappings will be provided by the originator node in their intermediate operation stack in the operation itself.
      this.remoteCache = AbstractDelegatingCache.unwrapCache(cacheComponentRef.running()).getAdvancedCache();
      // The iteration caches should only deal with local entries.
      this.cache = remoteCache.withFlags(Flag.CACHE_MODE_LOCAL);
      ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
      this.maxSegment = clusteringConfiguration.hash().numSegments();

      updateStrategy(configuration.persistence().usingSegmentedStore());
      persistenceManager.addStoreListener(storeChangeListener);
   }

   @Stop
   public void stop() {
      persistenceManager.removeStoreListener(storeChangeListener);
   }

   private void updateStrategy(boolean usingSegmentedStored) {
      if (configuration.persistence().usingStores() && !usingSegmentedStored) {
         localPublisherStrategy = nonSegmentedPublisher;
      } else {
         localPublisherStrategy = segmentedPublisher;
      }
   }

   @Override
   public <R> CompletionStage<PublisherResult<R>> keyReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (keysToInclude != null) {
         return handleSpecificKeys(parallelPublisher, keysToInclude, keysToExclude, explicitFlags, deliveryGuarantee,
               collator, finalizer);
      }

      CacheSet<K> keySet = getKeySet(explicitFlags);

      Function<K, K> toKeyFunction = Function.identity();
      switch (deliveryGuarantee) {
         case AT_MOST_ONCE:
            CompletionStage<R> stage = atMostOnce(parallelPublisher, keySet, keysToExclude, toKeyFunction,
                  segments, collator, finalizer);
            return stage.thenApply(ignoreSegmentsFunction());
         case AT_LEAST_ONCE:
            return atLeastOnce(parallelPublisher, keySet, keysToExclude, toKeyFunction, segments, collator, finalizer);
         case EXACTLY_ONCE:
            return exactlyOnce(parallelPublisher, keySet, keysToExclude, toKeyFunction, segments, collator, finalizer);
         default:
            throw new UnsupportedOperationException("Unsupported delivery guarantee: " + deliveryGuarantee);
      }
   }

   @Override
   public <R> CompletionStage<PublisherResult<R>> entryReduction(boolean parallelPublisher, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (keysToInclude != null) {
         return handleSpecificEntries(parallelPublisher, keysToInclude, keysToExclude, explicitFlags, deliveryGuarantee,
               collator, finalizer);
      }

      CacheSet<CacheEntry<K, V>> entrySet = getEntrySet(explicitFlags);

      // We have to cast to Function, since we can't cast our inner generic
      Function<CacheEntry<K, V>, K> toKeyFunction = (Function) StreamMarshalling.entryToKeyFunction();
      switch (deliveryGuarantee) {
         case AT_MOST_ONCE:
            CompletionStage<R> stage = atMostOnce(parallelPublisher, entrySet, keysToExclude, toKeyFunction,
                  segments, collator, finalizer);
            return stage.thenApply(ignoreSegmentsFunction());
         case AT_LEAST_ONCE:
            return atLeastOnce(parallelPublisher, entrySet, keysToExclude, toKeyFunction, segments, collator, finalizer);
         case EXACTLY_ONCE:
            return exactlyOnce(parallelPublisher, entrySet, keysToExclude, toKeyFunction, segments, collator, finalizer);
         default:
            throw new UnsupportedOperationException("Unsupported delivery guarantee: " + deliveryGuarantee);
      }
   }

   @Override
   public <R> SegmentAwarePublisherSupplier<R> keyPublisher(IntSet segments, Set<K> keysToInclude,
                                                            Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
                                                            Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      if (keysToInclude != null) {
         AdvancedCache<K, V> cache = getCache(deliveryGuarantee, explicitFlags);
         return specificKeyPublisher(segments, keysToInclude, keyFlowable -> keyFlowable.filter(cache::containsKey),
               transformer);
      }
      return new SegmentAwarePublisherSupplierImpl<>(segments, getKeySet(explicitFlags), Function.identity(),
                                                     keysToExclude, deliveryGuarantee, transformer);
   }

   private Flowable<CacheEntry<K, V>> filterEntries(AdvancedCache<K, V> cacheToUse, Flowable<K> entryFlowable) {
      return entryFlowable.concatMapMaybe(k -> {
         CompletableFuture<CacheEntry<K, V>> future = cacheToUse.getCacheEntryAsync(k);
         future = future.thenApply(entry -> {
            if (entry == null) {
               return NullCacheEntry.<K, V>getInstance();
            } else if (entry instanceof MVCCEntry) {
               // Scattered cache can return MVCCEntry instances
               entry = new ImmortalCacheEntry(entry.getKey(), entry.getValue());
            }
            return entry;
         });
         return Maybe.fromCompletionStage(future);
      }).filter(e -> e != NullCacheEntry.getInstance());
   }

   @Override
   public <R> SegmentAwarePublisherSupplier<R> entryPublisher(IntSet segments, Set<K> keysToInclude,
                                                              Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
                                                              Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
      if (keysToInclude != null) {
         AdvancedCache<K, V> cacheToUse = getCache(deliveryGuarantee, explicitFlags);
         return specificKeyPublisher(segments, keysToInclude, entryFlowable ->
                     filterEntries(cacheToUse, entryFlowable)
               , transformer);
      }
      return new SegmentAwarePublisherSupplierImpl<>(segments, getEntrySet(explicitFlags),
                                                     StreamMarshalling.entryToKeyFunction(), keysToExclude, deliveryGuarantee, transformer);
   }

   private <I, R> SegmentAwarePublisherSupplier<R> specificKeyPublisher(IntSet segments, Set<K> keysToInclude,
                                                                        FlowableConverter<K, Flowable<I>> conversionFunction,
                                                                        Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
      return new BaseSegmentAwarePublisherSupplier<R>() {
         @Override
         public Publisher<R> publisherWithoutSegments() {
            return Flowable.fromIterable(keysToInclude)
                  .to(conversionFunction)
                  .to(transformer::apply);
         }

         @Override
         Flowable<NotificationWithLost<R>> flowableWithNotifications(boolean reuseNotifications) {
            return Flowable.fromIterable(keysToInclude)
                  .groupBy(keyPartitioner::getSegment)
                  // We use concatMapEager instead of flatMap (groupBy needs either to prevent starvation) to ensure
                  // ordering guarantees defined in the LocalPublisherManager entryPublisher method.
                  // Due to eager subscription we cannot reuse notifications
                  .concatMapEager(group -> {
                     int segment = group.getKey();
                     // Shouldn't be possible to get a key that doesn't belong to the required segment - but just so
                     // we don't accidentally starve the groupBy
                     if (!segments.remove(segment)) {
                        throw new IllegalArgumentException("Key: " + blockingFirst(group) + " maps to segment: " + segment +
                              ", which was not included in segments provided: " + segments);
                     }
                     return Flowable.fromPublisher(conversionFunction.apply(group)
                                 .to(transformer::apply))
                           .map(r -> Notifications.value(r, segment))
                           .concatWith(Single.just(Notifications.segmentComplete(segment)));
                  }, segments.size(), Math.min(keysToInclude.size(), Flowable.bufferSize()))
                  .concatWith(Flowable.fromIterable(segments).map(Notifications::segmentComplete));
         }
      };
   }

   // This method is here for checkstyle, only reason we use this method is for throwing an exception, when we know
   // there is a guaranteed first value, so it will never actually block.
   @SuppressWarnings("checkstyle:forbiddenmethod")
   static Object blockingFirst(Flowable<?> flowable) {
      return flowable.blockingFirst();
   }

   private abstract static class BaseSegmentAwarePublisherSupplier<R> implements SegmentAwarePublisherSupplier<R> {
      @Override
      public Publisher<Notification<R>> publisherWithSegments() {
         return flowableWithNotifications(false).filter(notification -> !notification.isLostSegment())
               .map(n -> n);
      }

      @Override
      public Publisher<NotificationWithLost<R>> publisherWithLostSegments(boolean reuseNotifications) {
         return flowableWithNotifications(reuseNotifications);
      }

      abstract Flowable<NotificationWithLost<R>> flowableWithNotifications(boolean reuseNotifications);
   }

   private class SegmentAwarePublisherSupplierImpl<I, R> extends BaseSegmentAwarePublisherSupplier<R> {
      private final IntSet segments;
      private final CacheSet<I> cacheSet;
      private final Predicate<? super I> predicate;
      private final DeliveryGuarantee deliveryGuarantee;
      private final Function<? super Publisher<I>, ? extends Publisher<R>> transformer;

      private SegmentAwarePublisherSupplierImpl(IntSet segments, CacheSet<I> cacheSet,
                                        Function<? super I, K> toKeyFunction, Set<K> keysToExclude,
                                        DeliveryGuarantee deliveryGuarantee,
                                        Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
         this.segments = segments;
         this.cacheSet = cacheSet;
         this.predicate = keysToExclude != null ? v -> !keysToExclude.contains(toKeyFunction.apply(v)) : null;
         this.deliveryGuarantee = deliveryGuarantee;
         this.transformer = transformer;
      }

      @Override
      public Publisher<R> publisherWithoutSegments() {
         return Flowable.fromIterable(segments).concatMap(segment -> {
            Publisher<I> publisher = cacheSet.localPublisher(segment);
            if (predicate != null) {
               publisher = Flowable.fromPublisher(publisher)
                     .filter(predicate);
            }
            return transformer.apply(publisher);
         });
      }

      Flowable<NotificationWithLost<R>> flowableWithNotifications(boolean reuseNotifications) {
         switch (deliveryGuarantee) {
            case AT_MOST_ONCE:
               Notifications.NotificationBuilder<R> atMostBuilder = reuseNotifications ? Notifications.reuseBuilder() :
                     Notifications.newBuilder();
               return Flowable.fromIterable(segments)
                     .concatMap(segment -> {
                        Flowable<I> flowable = Flowable.fromPublisher(cacheSet.localPublisher(segment));
                        if (predicate != null) {
                           flowable = flowable.filter(predicate);
                        }
                        return flowable.compose(transformer::apply)
                              .map(r -> atMostBuilder.value(r, segment))
                              .concatWith(Single.fromSupplier(() -> atMostBuilder.segmentComplete(segment)));
                     });
            case AT_LEAST_ONCE:
            case EXACTLY_ONCE:
               // Need to use defer to have the shared variables between the various inner publishers but also
               // isolate between multiple subscriptions
               return Flowable.defer(() -> {
                  Notifications.NotificationBuilder<R> builder = reuseNotifications ? Notifications.reuseBuilder() :
                        Notifications.newBuilder();
                  IntSet concurrentSet = IntSets.concurrentCopyFrom(segments, maxSegment);
                  RemoveSegmentListener listener = new RemoveSegmentListener(concurrentSet);

                  changeListener.add(listener);

                  // Check topology before submitting
                  listener.verifyTopology(distributionManager.getCacheTopology());

                  return Flowable.fromIterable(segments).concatMap(segment -> {
                     if (!concurrentSet.contains(segment)) {
                        return Flowable.just(builder.segmentLost(segment));
                     }
                     Flowable<I> flowable = Flowable.fromPublisher(cacheSet.localPublisher(segment));
                     if (predicate != null) {
                        flowable = flowable.filter(predicate);
                     }
                     return flowable.compose(transformer::apply)
                           .map(r -> builder.value(r, segment))
                           .concatWith(Single.fromSupplier(() ->
                                 concurrentSet.remove(segment) ?
                                       builder.segmentComplete(segment) : builder.segmentLost(segment)
                           ));
                  }).doFinally(() -> changeListener.remove(listener));
               });
            default:
               throw new UnsupportedOperationException("Unsupported delivery guarantee: " + deliveryGuarantee);
         }
      }
   }

   @Override
   public void segmentsLost(IntSet lostSegments) {
      if (log.isTraceEnabled()) {
         log.tracef("Notifying listeners of lost segments %s", lostSegments);
      }
      changeListener.forEach(lostSegments::forEach);
   }

   @Override
   public CompletionStage<Long> sizePublisher(IntSet segments, long flags) {
      SizeCommand command = commandsFactory.buildSizeCommand(
            segments, EnumUtil.mergeBitSets(flags, FlagBitSets.CACHE_MODE_LOCAL));
      InvocationContext ctx = invocationContextFactory.createInvocationContext(false, UNBOUNDED);
      return CompletableFuture.completedFuture(invocationHelper.running().invoke(ctx, command));
   }

   private static Function<Object, PublisherResult<Object>> ignoreSegmentsFunction = value ->
         new SegmentPublisherResult<>(IntSets.immutableEmptySet(), value);

   static <R> Function<R, PublisherResult<R>> ignoreSegmentsFunction() {
      return (Function) ignoreSegmentsFunction;
   }

   private <I, R> void handleParallelSegment(PrimitiveIterator.OfInt segmentIter, int initialSegment, CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         FlowableProcessor<R> processor, IntSet concurrentSegments, SegmentListener listener) {
      try {
         while (true) {
            // The first run initialSegment will be 0 or greater. We use that segment and set it to -1 to notify
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
               // If we didn't lose the segment we can use its value, otherwise we just ignore it
               if (!listener.segmentsLost.contains(nextSegment)) {
                  R result = CompletionStages.join(stage);
                  if (result != null) {
                     processor.onNext(result);
                  }
               }
            } else {
               final FlowableProcessor<R> processorToUse = processor;
               stage.whenComplete((value, t) -> {
                  if (t != null) {
                     processorToUse.onError(t);
                  } else {
                     // If we complete the iteration try to remove the segment - so it can't be suspected
                     concurrentSegments.remove(nextSegment);
                     // This segment was lost before we could complete our iteration - so we have to discard the result
                     if (!listener.segmentsLost.contains(nextSegment) && value != null) {
                        processor.onNext(value);
                     }
                     handleParallelSegment(segmentIter, -1, set, keysToExclude, toKeyFunction, collator,
                           processor, concurrentSegments, listener);
                  }
               });
               return;
            }
         }
         processor.onComplete();
      } catch (Throwable t) {
         processor.onError(t);
      }
   }

   /**
    * Retrieves the next int from the iterator in a thread safe manner. This method
    * synchronizes on the iterator instance, so be sure not to mix this object monitor with other invocations.
    * If the iterator has been depleted this method will return -1 instead.
    *
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
      return localPublisherStrategy.exactlyOnceHandleLostSegments(finalValue, listener);
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
      return localPublisherStrategy.exactlyOnceParallel(set, keysToExclude, toKeyFunction, segments, collator, listener, concurrentSegments);
   }

   protected <I, R> Flowable<R> exactlyOnceSequential(CacheSet<I> set,
                                                      Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
                                                      Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
                                                      SegmentListener listener, IntSet concurrentSegments) {
      return localPublisherStrategy.exactlyOnceSequential(set, keysToExclude, toKeyFunction, segments, collator, listener, concurrentSegments);
   }

   private AdvancedCache<K, V> getCache(DeliveryGuarantee deliveryGuarantee, long explicitFlags) {
      AdvancedCache<K, V> cache = deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE ? this.cache : remoteCache;
      if (explicitFlags != EnumUtil.EMPTY_BIT_SET) {
         return cache.withFlags(EnumUtil.enumSetOf(explicitFlags, Flag.class));
      }
      return cache;
   }

   private <R> CompletionStage<PublisherResult<R>> handleSpecificKeys(boolean parallelPublisher, Set<K> keysToInclude,
         Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      AdvancedCache<K, V> cache = getCache(deliveryGuarantee, explicitFlags);
      return handleSpecificObjects(parallelPublisher, keysToInclude, keysToExclude, keyFlowable ->
            // Filter out all the keys that aren't in the cache
            keyFlowable.concatMapMaybe(key ->
               Maybe.fromCompletionStage(cache.containsKeyAsync(key)
                     .thenApply(contains -> contains ? key : null))
            )
      , collator, finalizer);
   }

   private <R> CompletionStage<PublisherResult<R>> handleSpecificEntries(boolean parallelPublisher, Set<K> keysToInclude,
         Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      AdvancedCache<K, V> cache = getCache(deliveryGuarantee, explicitFlags);
      return handleSpecificObjects(parallelPublisher, keysToInclude, keysToExclude, keyFlowable ->
                  keyFlowable.concatMapMaybe(k -> {
                     CompletableFuture<CacheEntry<K, V>> future = cache.getCacheEntryAsync(k);
                     future = future.thenApply(entry -> {
                        if (entry instanceof MVCCEntry) {
                           // Scattered cache can return MVCCEntry instances
                           entry = new ImmortalCacheEntry(entry.getKey(), entry.getValue());
                        }
                        return entry;
                     });
                     return Maybe.fromCompletionStage(future);
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
               .flatMapMaybe(keys -> {
                  // Due to window abandonment (check RxJava3 docs) we must subscribe synchronously and then
                  // observe on the publisher for parallelism
                  CompletionStage<R> stage = keyTransformer.apply(keys.observeOn(nonBlockingScheduler))
                        .to(collator::apply);
                  return Maybe.fromCompletionStage(stage);
               });
         return finalizer.apply(stageFlowable).thenApply(ignoreSegmentsFunction());
      } else {
         return keyTransformer.apply(keyFlowable)
               .to(collator::apply)
               .thenApply(ignoreSegmentsFunction());
      }
   }

   private <I, R> CompletionStage<R> parallelAtMostOnce(CacheSet<I> set, Set<K> keysToExclude,
         Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      Flowable<R> stageFlowable = Flowable.fromIterable(segments)
            .parallel(cpuCount)
            .runOn(nonBlockingScheduler)
            .concatMap(segment -> {
               Flowable<I> innerFlowable = Flowable.fromPublisher(set.localPublisher(segment));
               if (keysToExclude != null) {
                  innerFlowable = innerFlowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
               }
               // TODO Make the collator return a Flowable/Maybe
               CompletionStage<R> stage = collator.apply(innerFlowable);
               return Maybe.fromCompletionStage(stage).toFlowable();
            })
            .sequential();

      return finalizer.apply(stageFlowable);
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

   private <I, R> CompletionStage<PublisherResult<R>> atLeastOnce(boolean parallel, CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> collator,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      SegmentListener listener = new SegmentListener(segments);
      changeListener.add(listener);

      listener.verifyTopology(distributionManager.getCacheTopology());

      CompletionStage<R> stage = atMostOnce(parallel, set, keysToExclude, toKeyFunction, segments, collator, finalizer);
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

   private CacheSet<K> getKeySet(long explicitFlags) {
      KeySetCommand<?, ?> command = commandsFactory.buildKeySetCommand(explicitFlags);
      InvocationContext ctx = invocationContextFactory.createInvocationContext(false, UNBOUNDED);
      return invocationHelper.running().invoke(ctx, command);
   }

   private CacheSet<CacheEntry<K, V>> getEntrySet(long explicitFlags) {
      EntrySetCommand<?,?> command = commandsFactory.buildEntrySetCommand(explicitFlags);
      InvocationContext ctx = invocationContextFactory.createInvocationContext(false, UNBOUNDED);
      return invocationHelper.running().invoke(ctx, command);
   }

   class RemoveSegmentListener implements IntConsumer {
      private final IntSet segments;

      RemoveSegmentListener(IntSet segments) {
         this.segments = segments;
      }

      @Override
      public void accept(int segment) {
         if (segments.remove(segment)) {
            if (log.isTraceEnabled()) {
               log.tracef("Listener %s lost segment %d", this, segment);
            }
         }
      }

      void verifyTopology(LocalizedCacheTopology localizedCacheTopology) {
         for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
            int segment = segmentIterator.nextInt();
            if (!localizedCacheTopology.isSegmentReadOwner(segment)) {
               if (log.isTraceEnabled()) {
                  log.tracef("Listener %s lost segment %d before invocation", this, segment);
               }
               segmentIterator.remove();
            }
         }
      }
   }

   class SegmentListener implements IntConsumer {
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
            if (log.isTraceEnabled()) {
               log.tracef("Listener %s lost segment %d", this, segment);
            }
            segmentsLost.set(segment);
         }
      }

      void verifyTopology(LocalizedCacheTopology localizedCacheTopology) {
         for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
            int segment = segmentIterator.nextInt();
            if (!localizedCacheTopology.isSegmentReadOwner(segment)) {
               segmentsLost.set(segment);
            }
         }
      }
   }

   abstract class LocalEntryPublisherStrategy {
      abstract <I, R> Flowable<R> exactlyOnceParallel(CacheSet<I> set,
                                                      Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
                                                      Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
                                                      SegmentListener listener, IntSet concurrentSegments);

      abstract <I, R> Flowable<R> exactlyOnceSequential(CacheSet<I> set,
                                                        Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
                                                        Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
                                                        SegmentListener listener, IntSet concurrentSegments);

      abstract <R> CompletionStage<PublisherResult<R>> exactlyOnceHandleLostSegments(CompletionStage<R> finalValue, SegmentListener listener);
   }

   class NonSegmentedEntryPublisherStrategy extends LocalEntryPublisherStrategy {

      @Override
      <I, R> Flowable<R> exactlyOnceParallel(CacheSet<I> set, Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments, Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer, SegmentListener listener, IntSet concurrentSegments) {
         Flowable<I> flowable = Flowable.fromPublisher(set.localPublisher(segments));

         if (keysToExclude != null) {
            flowable = flowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
         }

         return flowable.buffer(PARALLEL_BATCH_SIZE)
               .parallel(cpuCount)
               .runOn(nonBlockingScheduler)
               .flatMap(buffer -> Flowable.fromCompletionStage(transformer.apply(Flowable.fromIterable(buffer))), false, cpuCount)
               .sequential();
      }

      @Override
      <I, R> Flowable<R> exactlyOnceSequential(CacheSet<I> set, Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments, Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer, SegmentListener listener, IntSet concurrentSegments) {
         Flowable<I> flowable = Flowable.fromPublisher(set.localPublisher(segments));

         if (keysToExclude != null) {
            flowable = flowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
         }
         return Flowable.fromCompletionStage(transformer.apply(flowable));
      }

      @Override
      <R> CompletionStage<PublisherResult<R>> exactlyOnceHandleLostSegments(CompletionStage<R> finalValue, SegmentListener listener) {
         return finalValue.thenApply(value -> {
            IntSet lostSegments = listener.segmentsLost;
            if (lostSegments.isEmpty()) {
               return LocalPublisherManagerImpl.<R>ignoreSegmentsFunction().apply(value);
            } else {
               // We treat all segments as being lost if any are lost in ours
               // NOTE: we never remove any segments from this set at all - so it will contain all requested segments
               return new SegmentPublisherResult<R>(listener.segments, null);
            }
         }).whenComplete((u, t) -> changeListener.remove(listener));
      }
   }

   class SegmentedLocalPublisherStrategyLocal extends LocalEntryPublisherStrategy {

      @Override
      public <I, R> Flowable<R> exactlyOnceParallel(CacheSet<I> set, Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments, Function<? super Publisher<I>, ? extends CompletionStage<R>> collator, SegmentListener listener, IntSet concurrentSegments) {
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
            nonBlockingScheduler.scheduleDirect(() ->
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

      @Override
      public <I, R> Flowable<R> exactlyOnceSequential(CacheSet<I> set, Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments, Function<? super Publisher<I>, ? extends CompletionStage<R>> collator, SegmentListener listener, IntSet concurrentSegments) {
         return Flowable.fromIterable(segments).concatMapMaybe(segment -> {
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
                  return Maybe.empty();
               }
               return Maybe.fromCompletionStage(stage);
            }

            return Maybe.fromCompletionStage(stage.thenCompose(value -> {
               // This means the segment was lost in the middle of processing
               if (listener.segmentsLost.contains(segment)) {
                  return CompletableFutures.completedNull();
               }
               return CompletableFuture.completedFuture(value);
            }));
         });
      }

      @Override
      public <R> CompletionStage<PublisherResult<R>> exactlyOnceHandleLostSegments(CompletionStage<R> finalValue, SegmentListener listener) {
         return handleLostSegments(finalValue, listener);
      }
   }
}
