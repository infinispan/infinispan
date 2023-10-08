package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager.StoreChangeListener;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.reactive.publisher.impl.commands.batch.CancelPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.NextPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.reactive.publisher.impl.commands.reduction.KeyPublisherResult;
import org.infinispan.reactive.publisher.impl.commands.reduction.PublisherResult;
import org.infinispan.reactive.publisher.impl.commands.reduction.ReductionPublisherRequestCommand;
import org.infinispan.reactive.publisher.impl.commands.reduction.SegmentPublisherResult;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

/**
 * ClusterPublisherManager that determines targets for the given segments and/or keys and then sends to local and
 * remote nodes in parallel collecting results to be returned. This implement prioritizes running as much as possible
 * on the local node, in some cases not even going remotely if all keys or segments are available locally.
 * @author wburns
 * @since 10.0
 */
@Scope(Scopes.NAMED_CACHE)
public class ClusterPublisherManagerImpl<K, V> implements ClusterPublisherManager<K, V> {
   protected final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject PublisherHandler publisherHandler;
   @Inject LocalPublisherManager<K, V> localPublisherManager;
   @Inject DistributionManager distributionManager;
   @Inject StateTransferLock stateTransferLock;
   @Inject RpcManager rpcManager;
   @Inject CommandsFactory commandsFactory;
   @Inject KeyPartitioner keyPartitioner;
   @Inject Configuration cacheConfiguration;
   @Inject ComponentRegistry componentRegistry;
   @Inject PersistenceManager persistenceManager;

   // Make sure we don't create one per invocation
   private final KeyComposedType KEY_COMPOSED = new KeyComposedType<>();
   private <R> KeyComposedType<R> keyComposedType() {
      return KEY_COMPOSED;
   }
   // Make sure we don't create one per invocation
   private final EntryComposedType ENTRY_COMPOSED = new EntryComposedType<>();

   private <R> EntryComposedType<R> entryComposedType() {
      return ENTRY_COMPOSED;
   }

   private final SizeComposedType SIZE_COMPOSED = new SizeComposedType();
   private SizeComposedType sizeComposedType() {
      return SIZE_COMPOSED;
   }

   private int maxSegment;
   private boolean replicatedCache;
   private volatile boolean writeBehindShared;
   // If they have a shared store we can just read everything locally
   private volatile boolean sharedStore;
   private final StoreChangeListener storeChangeListener = pm -> {
      writeBehindShared = pm.usingSharedAsyncStore();
      sharedStore = pm.usingSharedStore();
   };

   protected RpcOptions rpcOptions;

   @Start
   public void start() {
      maxSegment = cacheConfiguration.clustering().hash().numSegments();
      replicatedCache = cacheConfiguration.clustering().cacheMode().isReplicated();
      updateStoreInfo(cacheConfiguration.persistence());
      persistenceManager.addStoreListener(storeChangeListener);

      // Note we use a little extra wiggle room for the timeout of the remote invocation by increasing it by 3 times
      // normal. This is due to our responses requiring additional processing time (iteration serialization and normally
      // increased payloads)
      rpcOptions = new RpcOptions(DeliverOrder.NONE, cacheConfiguration.clustering().remoteTimeout() * 3,
            TimeUnit.MILLISECONDS);
      cacheConfiguration.clustering()
                   .attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
                   .addListener((a, ignored) -> {
                      rpcOptions = new RpcOptions(DeliverOrder.NONE, a.get().longValue() * 3, TimeUnit.MILLISECONDS);
                   });
   }

   @Stop
   public void stop() {
      persistenceManager.removeStoreListener(storeChangeListener);
   }

   @Override
   public <R> CompletionStage<R> keyReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         InvocationContext ctx, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return reduction(parallelPublisher, segments, keysToInclude, ctx, explicitFlags, deliveryGuarantee, keyComposedType(), transformer, finalizer);
   }

   @Override
   public <R> CompletionStage<R> entryReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         InvocationContext ctx, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return reduction(parallelPublisher, segments, keysToInclude, ctx, explicitFlags, deliveryGuarantee, entryComposedType(), transformer, finalizer);
   }

   private <I, R> CompletionStage<R> reduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude, InvocationContext ctx,
         long explicitFlags, DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      // Needs to be serialized processor as we can write to it from different threads
      FlowableProcessor<R> flowableProcessor = UnicastProcessor.<R>create().toSerialized();
      // Apply the finalizer first (which subscribes) before emitting items, to avoid buffering in UnicastProcessor
      CompletionStage<R> stage = finalizer.apply(flowableProcessor);

      Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizerToUse =
            requiresFinalizer(parallelPublisher, keysToInclude, deliveryGuarantee) ? finalizer : null;

      if (keysToInclude != null) {
         startKeyPublisher(parallelPublisher, segments, keysToInclude, ctx, explicitFlags, deliveryGuarantee,
               composedType, transformer, finalizerToUse, flowableProcessor);
      } else {
         startSegmentPublisher(parallelPublisher, segments, ctx, explicitFlags,
               deliveryGuarantee, composedType, transformer, finalizerToUse, flowableProcessor);
      }
      return stage;
   }

   /**
    * This method is used to determine if a finalizer is required to be sent remotely. For cases we don't have to
    * we don't want to serialize it for nothing
    * @return whether finalizer is required
    */
   private <R> boolean requiresFinalizer(boolean parallelPublisher, Set<K> keysToInclude,
         DeliveryGuarantee deliveryGuarantee) {
      // Parallel publisher has to use the finalizer to consolidate intermediate values on the remote nodes
      return parallelPublisher ||
            // Using segments with exactly once does one segment at a time and requires consolidation
            keysToInclude == null && deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE;
   }

   private <I, R> void handleContextInvocation(IntSet segments, Set<K> keysToInclude, InvocationContext ctx, ComposedType<K, I, R> composedType,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         BiConsumer<PublisherResult<R>, Throwable> biConsumer) {
      CompletionStage<PublisherResult<R>> localStage = composedType.contextInvocation(segments, keysToInclude, ctx,
            transformer);

      if (log.isTraceEnabled()) {
         // Make sure the trace occurs before response is processed
         localStage = localStage.whenComplete((results, t) -> {
            if (t != null) {
               log.tracef(t, "Received exception while processing context %s", ctx);
            } else {
               log.tracef("Result was: %s for context %s", results.getResult(), ctx);
            }
         });
      }

      // Finally report the result to the BiConsumer so it knows the result
      localStage.whenComplete(biConsumer);
   }

   // Finalizer isn't required as the FlowableProcessor already has that configured upstream
   private static <I, R> void handleNoTargets(Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         FlowableProcessor<R> flowableProcessor) {
      // Need to do this in case if the transformer or finalizer has additional values such as reduce with identity
      // or switchIfEmpty etc.
      CompletionStage<R> transformedStage = transformer.apply(Flowable.empty());
      transformedStage.whenComplete((value, t) -> {
         if (t != null) {
            flowableProcessor.onError(t);
         } else {
            if (value != null) {
               flowableProcessor.onNext(value);
            }
            flowableProcessor.onComplete();
         }
      });
   }

   private <I, R> void startKeyPublisher(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude, InvocationContext ctx,
         long explicitFlags, DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer,
         FlowableProcessor<R> flowableProcessor) {
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      Address localAddress = topology.getLocalAddress();
      // This excludes the keys from the various address targets
      Map<Address, Set<K>> keyTargets = determineKeyTargets(topology, keysToInclude, localAddress, segments, ctx);

      int keyTargetSize = keyTargets.size();

      if (keyTargetSize == 0) {
         handleNoTargets(transformer, flowableProcessor);
         return;
      }

      AtomicInteger parallelCount;
      boolean useContext = ctx != null && ctx.lookedUpEntriesCount() > 0;
      if (useContext) {
         parallelCount = new AtomicInteger(keyTargetSize + 1);
      } else {
         parallelCount = new AtomicInteger(keyTargetSize);
      }

      // This way we only have to allocate 1 per request chain
      BiConsumer<PublisherResult<R>, Throwable> biConsumer = new KeyBiConsumer<>(flowableProcessor,
            parallelCount, topology.getTopologyId(), parallelPublisher, explicitFlags, deliveryGuarantee,
            composedType, transformer, finalizer);

      Set<K> localKeys = keyTargets.remove(localAddress);
      // If any targets left, they are all remote
      if (!keyTargets.isEmpty()) {
         // We submit the remote ones first as they will not block at all, just to send remote tasks
         for (Map.Entry<Address, Set<K>> remoteTarget : keyTargets.entrySet()) {
            Address remoteAddress = remoteTarget.getKey();
            Set<K> remoteKeys = remoteTarget.getValue();
            TopologyAffectedCommand command = composedType.remoteInvocation(parallelPublisher, null, remoteKeys,
                  null, explicitFlags, deliveryGuarantee, transformer, finalizer);
            command.setTopologyId(topology.getTopologyId());
            CompletionStage<PublisherResult<R>> stage = rpcManager.invokeCommand(remoteAddress, command,
                  new KeyPublisherResultCollector<>(remoteKeys), rpcManager.getSyncRpcOptions());
            stage.whenComplete(biConsumer);
         }
      }

      if (localKeys != null) {
         DeliveryGuarantee guarantee = deliveryToUse(null, deliveryGuarantee, explicitFlags);
         CompletionStage<PublisherResult<R>> localStage = composedType.localInvocation(parallelPublisher, null,
               localKeys, null, explicitFlags, guarantee, transformer, finalizer);

         if (log.isTraceEnabled()) {
            // Make sure the trace occurs before response is processed
            localStage = localStage.whenComplete((results, t) -> {
               if (t != null) {
                  log.tracef(t, "Received exception while processing keys %s from %s", localKeys, localAddress);
               } else {
                  log.tracef("Result was: %s for keys %s from %s with %s suspected keys",
                        results.getResult(), localKeys, localAddress, results.getSuspectedKeys());
               }
            });
         }

         // Map to the same collector, so we can reuse the same BiConsumer
         localStage.whenComplete(biConsumer);
      }

      if (useContext) {
         handleContextInvocation(segments, keysToInclude, ctx, composedType, transformer, biConsumer);
      }
   }

   private <I, R> void startSegmentPublisher(boolean parallelPublisher, IntSet segments,
         InvocationContext ctx, long explicitFlags, DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer,
         FlowableProcessor<R> flowableProcessor) {
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      Address localAddress = topology.getLocalAddress();
      Map<Address, IntSet> targets = determineSegmentTargets(topology, segments, localAddress, explicitFlags);

      int targetSize = targets.size();

      if (targetSize == 0) {
         handleNoTargets(transformer, flowableProcessor);
         return;
      }

      // used to determine that last parallel completion, to either complete or retry
      AtomicInteger parallelCount;
      boolean useContext = ctx != null && ctx.lookedUpEntriesCount() > 0;
      Map<Address, Set<K>> keysToExcludeByAddress;
      if (useContext) {
         parallelCount = new AtomicInteger(targetSize + 1);
         keysToExcludeByAddress = determineKeyTargets(topology, (Set<K>) ctx.getLookedUpEntries().keySet(), localAddress,
               segments, null);
      } else {
         parallelCount = new AtomicInteger(targetSize);
         keysToExcludeByAddress = Collections.emptyMap();
      }

      // This way we only have to allocate 1 per request chain
      BiConsumer<PublisherResult<R>, Throwable> biConsumer = new SegmentSpecificConsumer<>(flowableProcessor,
            parallelCount, topology.getTopologyId(), parallelPublisher, ctx, explicitFlags, deliveryGuarantee,
            composedType, transformer, finalizer);

      IntSet localSegments = targets.remove(localAddress);

      // If any targets left, they are all remote
      if (!targets.isEmpty()) {
         // We submit the remote ones first as they will not block at all, just to send remote tasks
         for (Map.Entry<Address, IntSet> remoteTarget : targets.entrySet()) {
            Address remoteAddress = remoteTarget.getKey();
            IntSet remoteSegments = remoteTarget.getValue();
            TopologyAffectedCommand command = composedType.remoteInvocation(parallelPublisher, remoteSegments, null,
                  keysToExcludeByAddress.get(remoteAddress), explicitFlags, deliveryGuarantee, transformer, finalizer);
            command.setTopologyId(topology.getTopologyId());
            CompletionStage<PublisherResult<R>> stage = rpcManager.invokeCommand(remoteAddress, command,
                  new SegmentPublisherResultCollector<>(remoteSegments), rpcManager.getSyncRpcOptions());
            stage.whenComplete(biConsumer);
         }
      }

      if (localSegments != null) {
         DeliveryGuarantee guarantee = deliveryToUse(null, deliveryGuarantee, explicitFlags);
         CompletionStage<PublisherResult<R>> localStage = composedType.localInvocation(parallelPublisher, localSegments,
               null, keysToExcludeByAddress.get(localAddress), explicitFlags, guarantee, transformer, finalizer);

         if (log.isTraceEnabled()) {
            // Make sure the trace occurs before response is processed
            localStage = localStage.whenComplete((results, t) -> {
               if (t != null) {
                  log.tracef(t, "Received exception while processing segments %s from %s", localSegments, localAddress);
               } else {
                  log.tracef("Result was: %s for segments %s from %s with %s suspected segments",
                        results.getResult(), localSegments, localAddress, results.getSuspectedSegments());
               }
            });
         }

         // Map to the same collector, so we can reuse the same BiConsumer
         localStage.whenComplete(biConsumer);
      }

      if (useContext) {
         handleContextInvocation(segments, null, ctx, composedType, transformer, biConsumer);
      }
   }

   private class SegmentSpecificConsumer<I, R> implements BiConsumer<PublisherResult<R>, Throwable> {
      private final FlowableProcessor<R> flowableProcessor;
      private final AtomicInteger parallelCount;

      private final IntSet segmentsToRetry = IntSets.concurrentSet(maxSegment);

      private final int currentTopologyId;
      private final boolean parallelPublisher;
      private final InvocationContext ctx;
      private final long explicitFlags;
      private final DeliveryGuarantee deliveryGuarantee;
      private final ComposedType<K, I, R> composedType;
      private final Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer;
      private final Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer;

      SegmentSpecificConsumer(FlowableProcessor<R> flowableProcessor, AtomicInteger parallelCount,
            int currentTopologyId, boolean parallelPublisher, InvocationContext ctx, long explicitFlags,
            DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         this.flowableProcessor = flowableProcessor;
         this.parallelCount = parallelCount;

         this.currentTopologyId = currentTopologyId;
         this.parallelPublisher = parallelPublisher;
         this.ctx = ctx;
         this.explicitFlags = explicitFlags;
         this.deliveryGuarantee = deliveryGuarantee;
         this.composedType = composedType;
         this.transformer = transformer;
         this.finalizer = finalizer;
      }

      @Override
      public void accept(PublisherResult<R> resultCollector, Throwable t) {
         if (t != null) {
            if (log.isTraceEnabled()) {
               log.tracef(t, "General error encountered when executing publisher request command");
            }
            flowableProcessor.onError(t);
         } else {
            handleResult(resultCollector);

            // We were the last one to complete if zero, so we have to complete or resubmit
            if (parallelCount.decrementAndGet() == 0) {
               onCompletion();
            }
         }
      }

      private void handleResult(PublisherResult<R> result) {
         IntSet suspectedSegments = result.getSuspectedSegments();
         if (suspectedSegments != null && !suspectedSegments.isEmpty()) {
            segmentsToRetry.addAll(suspectedSegments);
         }

         R actualValue = result.getResult();
         if (actualValue != null) {
            flowableProcessor.onNext(actualValue);
         }
      }

      private void onCompletion() {
         if (segmentsToRetry.isEmpty()) {
            flowableProcessor.onComplete();
         } else {
            int nextTopology = currentTopologyId + 1;
            if (log.isTraceEnabled()) {
               log.tracef("Retrying segments %s after %d is installed", segmentsToRetry, nextTopology);
            }
            // If we had an issue with segments, we need to wait until the next topology is installed to try again
            stateTransferLock.topologyFuture(nextTopology).whenComplete((ign, innerT) -> {
               if (innerT != null) {
                  if (log.isTraceEnabled()) {
                     log.tracef(innerT, "General error encountered when waiting on topology future for publisher request command");
                  }
                  flowableProcessor.onError(innerT);
               } else {
                  // Restart with the missing segments
                  startSegmentPublisher(parallelPublisher, segmentsToRetry, ctx, explicitFlags, deliveryGuarantee,
                        composedType, transformer, finalizer, flowableProcessor);
               }
            });
         }
      }
   }

   private class KeyBiConsumer<I, R> implements BiConsumer<PublisherResult<R>, Throwable> {
      private final FlowableProcessor<R> flowableProcessor;
      private final AtomicInteger parallelCount;

      private final Set<K> keysToRetry = ConcurrentHashMap.newKeySet();

      private final int currentTopologyId;
      private final boolean parallelPublisher;
      private final long explicitFlags;
      private final DeliveryGuarantee deliveryGuarantee;
      private final ComposedType<K, I, R> composedType;
      private final Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer;
      private final Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer;

      KeyBiConsumer(FlowableProcessor<R> flowableProcessor, AtomicInteger parallelCount, int currentTopologyId,
            boolean parallelPublisher, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            ComposedType<K, I, R> composedType, Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         this.flowableProcessor = flowableProcessor;
         this.parallelCount = parallelCount;
         this.currentTopologyId = currentTopologyId;
         this.parallelPublisher = parallelPublisher;
         this.explicitFlags = explicitFlags;
         this.deliveryGuarantee = deliveryGuarantee;
         this.composedType = composedType;
         this.transformer = transformer;
         this.finalizer = finalizer;
      }

      @Override
      public void accept(PublisherResult<R> resultCollector, Throwable t) {
         if (t != null) {
            if (log.isTraceEnabled()) {
               log.tracef(t, "General error encountered when executing publisher request command");
            }
            flowableProcessor.onError(t);
         } else {
            handleResult(resultCollector);

            // We were the last one to complete if zero, so we have to complete
            if (parallelCount.decrementAndGet() == 0) {
               onCompletion();
            }
         }
      }

      private void handleResult(PublisherResult<R> result) {
         // DistributedCacheStream.reduce(accumulator) works with null as defaultItem when identity is not given
         //  -> So null is a valid actualValue and does not mean we have suspect keys
         Set<?> suspectedKeys = result.getSuspectedKeys();
         if (suspectedKeys != null && !suspectedKeys.isEmpty()) {
            keysToRetry.addAll((Set) suspectedKeys);
         }

         R actualValue = result.getResult();
         if (actualValue != null) {
            flowableProcessor.onNext(actualValue);
         }
      }

      private void onCompletion() {
         if (keysToRetry.isEmpty()) {
            flowableProcessor.onComplete();
         } else {
            int nextTopology = currentTopologyId + 1;
            if (log.isTraceEnabled()) {
               log.tracef("Retrying keys %s after %d is installed", keysToRetry, nextTopology);
            }
            // If we had an issue with segments, we need to wait until the next topology is installed to try again
            stateTransferLock.topologyFuture(nextTopology).whenComplete((ign, innerT) -> {
               if (innerT != null) {
                  if (log.isTraceEnabled()) {
                     log.tracef(innerT, "General error encountered when waiting on topology future for publisher request command");
                  }
                  flowableProcessor.onError(innerT);
               } else {
                  // Restart with keys that were missing - note that segments and exclude is always null - as we
                  // already filtered those out in the first startKeyPublisher invocation
                  startKeyPublisher(parallelPublisher, null, keysToRetry, null, explicitFlags, deliveryGuarantee,
                        composedType, transformer, finalizer, flowableProcessor);
               }
            });
         }
      }
   }

   private class KeyPublisherResultCollector<R> extends ValidResponseCollector<PublisherResult<R>> {
      private final Set<K> keys;

      KeyPublisherResultCollector(Set<K> keys) {
         this.keys = keys;
      }

      @Override
      public PublisherResult<R> finish() {
         throw new IllegalStateException("Should never be invoked!");
      }

      @Override
      protected PublisherResult<R> addValidResponse(Address sender, ValidResponse response) {
         PublisherResult<R> results = response.getResponseObject();
         if (log.isTraceEnabled()) {
            log.tracef("Result was: %s for keys %s from %s", results.getResult(), keys, sender);
         }
         return results;
      }

      @Override
      protected PublisherResult<R> addTargetNotFound(Address sender) {
         if (log.isTraceEnabled()) {
            log.tracef("Cache is no longer running for keys %s from %s - must retry", Util.toStr(keys), sender);
         }
         return new KeyPublisherResult<>(keys);
      }

      @Override
      protected PublisherResult<R> addException(Address sender, Exception exception) {
         if (log.isTraceEnabled()) {
            log.tracef(exception, "Exception encountered while requesting keys %s from %s", Util.toStr(keys), sender);
         }
         // Throw the exception so it is propagated to caller
         if (exception instanceof CacheException) {
            throw (CacheException) exception;
         }
         throw new CacheException(exception);
      }
   }

   private class SegmentPublisherResultCollector<R> extends ValidResponseCollector<PublisherResult<R>> {
      private final IntSet targetSegments;

      SegmentPublisherResultCollector(IntSet targetSegments) {
         this.targetSegments = targetSegments;
      }

      @Override
      public PublisherResult<R> finish() {
         throw new IllegalStateException("Should never be invoked!");
      }

      @Override
      protected PublisherResult<R> addValidResponse(Address sender, ValidResponse response) {
         Object value = response.getResponseValue();
         if (value instanceof PublisherResult) {
            PublisherResult<R> results = (PublisherResult<R>) value;
            if (log.isTraceEnabled()) {
               log.tracef("Result was: %s for segments %s from %s with %s suspected segments", results.getResult(),
                     targetSegments, sender, results.getSuspectedSegments());
            }
            return results;
         }

         if (log.isTraceEnabled()) {
            log.tracef("Result was: %s for segments %s from %s.", value, targetSegments, sender);
         }

         return new SegmentPublisherResult<>(null, (R) value);
      }

      @Override
      protected PublisherResult<R> addTargetNotFound(Address sender) {
         if (log.isTraceEnabled()) {
            log.tracef("Cache is no longer running for segments %s from %s - must retry", targetSegments, sender);
         }
         return new SegmentPublisherResult<>(targetSegments, null);
      }

      @Override
      protected PublisherResult<R> addException(Address sender, Exception exception) {
         if (log.isTraceEnabled()) {
            log.tracef(exception, "Exception encountered while requesting segments %s from %s", targetSegments, sender);
         }
         // Throw the exception so it is propagated to caller
         if (exception instanceof CacheException) {
            throw (CacheException) exception;
         }
         throw new CacheException(exception);
      }
   }

   private Map<Address, IntSet> determineSegmentTargets(LocalizedCacheTopology topology, IntSet segments, Address localAddress, long explicitFlags) {
      if (skipRemoteInvocation(explicitFlags)) {
         // A shared store without write behind will have all values available on all nodes, so just do local lookup
         var map = new HashMap<Address, IntSet>();
         map.put(localAddress, segments == null ? IntSets.immutableRangeSet(maxSegment) : segments);
         return map;
      }
      Map<Address, IntSet> targets = new HashMap<>();
      if (segments == null) {
         for (int segment = 0; segment < maxSegment; ++segment) {
            handleSegment(segment, topology, localAddress, targets);
         }
      } else {
         for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
            int segment = iter.nextInt();
            handleSegment(segment, topology, localAddress, targets);
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Targets determined to be %s on topology " + topology.getTopologyId(), targets);
      }
      return targets;
   }

   private void handleSegment(int segment, LocalizedCacheTopology topology, Address localAddress,
         Map<Address, IntSet> targets) {
      DistributionInfo distributionInfo = topology.getSegmentDistribution(segment);

      Address targetAddres = determineOwnerToReadFrom(distributionInfo, localAddress);
      // Scattered cache can have a state when it has no primary owner - thus we ignore those segments. The retry
      // will wait for a new topology to try again
      if (targetAddres != null) {
         addToMap(targets, targetAddres, segment);
      } else if (log.isTraceEnabled()) {
         log.tracef("No owner was found for segment %s.", segment);
      }
   }

   private void addToMap(Map<Address, IntSet> map, Address owner, int segment) {
      IntSet set = map.get(owner);
      if (set == null) {
         set = IntSets.mutableEmptySet(maxSegment);
         map.put(owner, set);
      }
      set.set(segment);
   }

   private Address determineOwnerToReadFrom(DistributionInfo distributionInfo, Address localAddress) {
      // Prioritize local node even if it is backup when we don't have a shared write behind store
      if (!writeBehindShared && distributionInfo.isReadOwner()) {
         return localAddress;
      } else {
         return distributionInfo.primary();
      }
   }

   private Map<Address, Set<K>> determineKeyTargets(LocalizedCacheTopology topology, Set<K> keys, Address localAddress,
         IntSet segments, InvocationContext ctx) {
      Map<Address, Set<K>> filteredKeys = new HashMap<>();
      for (K key : keys) {
         if (ctx != null && ctx.lookupEntry(key) != null) {
            continue;
         }
         DistributionInfo distributionInfo = topology.getDistribution(key);
         if (segments != null && !segments.contains(distributionInfo.segmentId())) {
            continue;
         }
         addToMap(filteredKeys, determineOwnerToReadFrom(distributionInfo, localAddress), key);
      }
      return filteredKeys;
   }

   private void addToMap(Map<Address, Set<K>> map, Address owner, K key) {
      Set<K> set = map.get(owner);
      if (set == null) {
         set = new HashSet<>();
         map.put(owner, set);
      }
      set.add(key);
   }

   interface ComposedType<K, I, R> {
      CompletionStage<PublisherResult<R>> localInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

      TopologyAffectedCommand remoteInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

      CompletionStage<PublisherResult<R>> contextInvocation(IntSet segments, Set<K> keysToInclude, InvocationContext ctx,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer);

      boolean isEntry();

      K toKey(I value);

      I fromCacheEntry(CacheEntry entry);
   }

   private class KeyComposedType<R> implements ComposedType<K, K, R> {

      @Override
      public CompletionStage<PublisherResult<R>> localInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return localPublisherManager.keyReduction(parallelPublisher, segments, keysToInclude, keysToExclude,
               explicitFlags, deliveryGuarantee, transformer, finalizer);
      }

      @Override
      public ReductionPublisherRequestCommand<K> remoteInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return commandsFactory.buildKeyReductionPublisherCommand(parallelPublisher, deliveryGuarantee, segments, keysToInclude,
               keysToExclude, explicitFlags, transformer, finalizer);
      }

      @Override
      public CompletionStage<PublisherResult<R>> contextInvocation(IntSet segments, Set<K> keysToInclude,
            InvocationContext ctx, Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer) {

         Flowable<K> flowable = LocalClusterPublisherManagerImpl.entryPublisherFromContext(ctx, segments, keyPartitioner, keysToInclude)
               .map(RxJavaInterop.entryToKeyFunction());
         return transformer.apply(flowable)
               .thenApply(LocalPublisherManagerImpl.ignoreSegmentsFunction());
      }

      @Override
      public boolean isEntry() {
         return false;
      }

      @Override
      public K toKey(K value) {
         return value;
      }

      @Override
      public K fromCacheEntry(CacheEntry entry) {
         return (K) entry.getKey();
      }
   }

   private class EntryComposedType<R> implements ComposedType<K, CacheEntry<K, V>, R> {

      @Override
      public CompletionStage<PublisherResult<R>> localInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return localPublisherManager.entryReduction(parallelPublisher, segments, keysToInclude, keysToExclude,
               explicitFlags, deliveryGuarantee, transformer, finalizer);
      }

      @Override
      public ReductionPublisherRequestCommand<K> remoteInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return commandsFactory.buildEntryReductionPublisherCommand(parallelPublisher, deliveryGuarantee, segments, keysToInclude,
               keysToExclude, explicitFlags, transformer, finalizer);
      }

      @Override
      public CompletionStage<PublisherResult<R>> contextInvocation(IntSet segments, Set<K> keysToInclude,
            InvocationContext ctx, Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer) {
         return transformer.apply(LocalClusterPublisherManagerImpl.entryPublisherFromContext(ctx, segments, keyPartitioner, keysToInclude))
               .thenApply(LocalPublisherManagerImpl.ignoreSegmentsFunction());
      }

      @Override
      public boolean isEntry() {
         return true;
      }

      @Override
      public K toKey(CacheEntry<K, V> value) {
         return value.getKey();
      }

      @Override
      public CacheEntry<K, V> fromCacheEntry(CacheEntry entry) {
         return entry;
      }
   }

   private class SizeComposedType implements ComposedType<K, Long, Long> {

      @Override
      public CompletionStage<PublisherResult<Long>> localInvocation(boolean parallelPublisher,
         IntSet segments, Set<K> keysToInclude, Set<K> keysToExclude, long explicitFlags,
         DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<Long>, ? extends CompletionStage<Long>> transformer,
         Function<? super Publisher<Long>, ? extends CompletionStage<Long>> finalizer) {
         return localPublisherManager.sizePublisher(segments, explicitFlags)
               .thenApply(size -> new SegmentPublisherResult<>(null, size));
      }

      @Override
      public SizeCommand remoteInvocation(boolean parallelPublisher, IntSet segments,
                                          Set<K> keysToInclude, Set<K> keysToExclude,
                                          long explicitFlags,
                                          DeliveryGuarantee deliveryGuarantee,
                                          Function<? super Publisher<Long>, ? extends CompletionStage<Long>> transformer,
                                          Function<? super Publisher<Long>, ? extends CompletionStage<Long>> finalizer) {
         return commandsFactory.buildSizeCommand(segments, EnumUtil.mergeBitSets(explicitFlags, FlagBitSets.CACHE_MODE_LOCAL));
      }

      @Override
      public CompletionStage<PublisherResult<Long>> contextInvocation(IntSet segments, Set<K> keysToInclude,
         InvocationContext ctx, Function<? super Publisher<Long>, ? extends CompletionStage<Long>> transformer) {
         throw new IllegalStateException("Should never be invoked!");
      }

      @Override
      public boolean isEntry() {
         return false;
      }

      @Override
      public K toKey(Long value) {
         throw new IllegalStateException("Should never be invoked!");
      }

      @Override
      public Long fromCacheEntry(CacheEntry entry) {
         throw new IllegalStateException("Should never be invoked!");
      }
   }

   private void updateStoreInfo(PersistenceConfiguration persistenceConfiguration) {
      for (StoreConfiguration storeConfiguration : persistenceConfiguration.stores()) {
         if (storeConfiguration.shared()) {
            sharedStore = true;
            if (storeConfiguration.async().enabled()) {
               writeBehindShared = true;
            }
         }
      }
   }

   @Override
   public <R> SegmentPublisherSupplier<R> keyPublisher(IntSet segments, Set<K> keysToInclude,
                                                       InvocationContext invocationContext, long explicitFlags,
                                                       DeliveryGuarantee deliveryGuarantee, int batchSize,
                                                       Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      if (keysToInclude != null) {
         return new KeyAwarePublisherImpl<>(keysToInclude, keyComposedType(), segments, invocationContext,
                                            explicitFlags, deliveryGuarantee, batchSize, transformer);
      }
      return new SegmentAwarePublisherImpl<>(segments, keyComposedType(), invocationContext, explicitFlags,
                                             deliveryGuarantee, batchSize, transformer);
   }

   @Override
   public <R> SegmentPublisherSupplier<R> entryPublisher(IntSet segments, Set<K> keysToInclude,
                                                         InvocationContext invocationContext, long explicitFlags,
                                                         DeliveryGuarantee deliveryGuarantee, int batchSize,
                                                         Function<? super Publisher<CacheEntry<K, V>>, ?
                                                               extends Publisher<R>> transformer) {
      if (keysToInclude != null) {
         return new KeyAwarePublisherImpl<>(keysToInclude, entryComposedType(), segments, invocationContext,
                                            explicitFlags, deliveryGuarantee, batchSize, transformer);
      }
      return new SegmentAwarePublisherImpl<>(segments, entryComposedType(), invocationContext, explicitFlags,
                                             deliveryGuarantee, batchSize, transformer);
   }

   @Override
   public CompletionStage<Long> sizePublisher(IntSet segments, InvocationContext ctx, long flags) {
      return reduction(false, segments, null, ctx, flags, DeliveryGuarantee.EXACTLY_ONCE,
            sizeComposedType(), null, PublisherReducers.add());
   }

   private final static AtomicInteger requestCounter = new AtomicInteger();

   private final static Function<ValidResponse, PublisherResponse> responseHandler = vr -> {
      if (vr instanceof SuccessfulResponse) {
         return vr.getResponseObject();
      } else {
         throw new IllegalArgumentException("Unsupported response received: " + vr);
      }
   };

   // We only allow 4 concurrent inner publishers to subscribe at a given time (arbitrary to keep request count down
   // but also provide adequate concurrent processing)
   private static final int MAX_INNER_SUBSCRIBERS = 4;

   /**
    * This class handles whenever a new subscriber is registered. This class handles the retry mechanism and submission
    * of requests to various nodes. All details regarding a specific subscriber should be stored in this class, such
    * as the completed segments.
    * @param <I>
    * @param <R>
    */
   class SubscriberHandler<I, R> implements ObjIntConsumer<I> {
      final AbstractSegmentAwarePublisher<I, R> publisher;
      final String requestId;

      final AtomicReferenceArray<Collection<K>> keysBySegment;
      final IntSet segmentsToComplete;
      // Only allow the first child publisher to use the context values
      final AtomicBoolean useContext = new AtomicBoolean(true);

      // Variable used to ensure we only read the context once - so it is not read again during a retry
      volatile int currentTopology = -1;

      SubscriberHandler(AbstractSegmentAwarePublisher<I, R> publisher, boolean withSegments) {
         this.publisher = publisher;
         this.requestId = rpcManager.getAddress() + "#" + requestCounter.incrementAndGet();

         this.keysBySegment = publisher.deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE ?
               new AtomicReferenceArray<>(maxSegment) : null;
         this.segmentsToComplete = IntSets.concurrentCopyFrom(publisher.segments, maxSegment);
      }

      /**
       * This method creates a Flowable that when published to will return the values while ensuring retries are
       * performed to guarantee the configured delivery guarantees.
       * <p>
       * This method starts up to 4 concurrent inner
       * subscriptions at the same time. These subscriptions will request values from the target node with the given
       * segments. If the local node is requested it is given to the last subscription to ensure the others are subscribed
       * to first, to allow for concurrent processing reliably.
       * <p>
       * An inner subscriber will publish their returned values and whether a segment has been completed or not to
       * us. When all subscribers have completed we see if all segments have been completed, if not we restart
       * the entire process again with the segments that haven't yet completed.
       */
      private <E> Flowable<E> getValuesFlowable(BiFunction<InnerPublisherSubscription.InnerPublisherSubscriptionBuilder<K, I, R>, Map.Entry<Address, IntSet>, Publisher<E>> subToFlowableFunction) {
         return Flowable.defer(() -> {
            if (!componentRegistry.getStatus().allowInvocations() && !componentRegistry.getStatus().startingUp()) {
               return Flowable.error(new IllegalLifecycleStateException());
            }
            LocalizedCacheTopology topology = distributionManager.getCacheTopology();
            int previousTopology = currentTopology;
            // Store the current topology in case if we have to retry
            int currentTopology = topology.getTopologyId();
            this.currentTopology = currentTopology;
            Address localAddress = rpcManager.getAddress();
            Map<Address, IntSet> targets = determineSegmentTargets(topology, segmentsToComplete, localAddress, publisher.explicitFlags);
            if (previousTopology != -1 && previousTopology == currentTopology || targets.isEmpty()) {
               int nextTopology = currentTopology + 1;
               if (log.isTraceEnabled()) {
                  log.tracef("Request id %s needs a new topology to retry segments %s. Current topology is %d, with targets %s",
                        requestId, segmentsToComplete, currentTopology, targets);
               }
               // When this is complete - the retry below will kick in again and we will have a new topology
               return RxJavaInterop.voidCompletionStageToFlowable(stateTransferLock.topologyFuture(nextTopology), true);
            }
            IntSet localSegments = targets.remove(localAddress);
            Iterator<Map.Entry<Address, IntSet>> iterator = targets.entrySet().iterator();
            Supplier<Map.Entry<Address, IntSet>> targetSupplier = () -> {
               synchronized (this) {
                  if (iterator.hasNext()) {
                     return iterator.next();
                  }
                  return null;
               }
            };

            Map<Address, Set<K>> excludedKeys;
            if (publisher.invocationContext == null) {
               excludedKeys = Collections.emptyMap();
            } else {
               excludedKeys = determineKeyTargets(topology,
                     (Set<K>) publisher.invocationContext.getLookedUpEntries().keySet(), localAddress,
                     segmentsToComplete, null);
            }

            int concurrentPublishers = Math.min(MAX_INNER_SUBSCRIBERS, targets.size() + (localSegments != null ? 1 : 0));

            int targetBatchSize = (publisher.batchSize + concurrentPublishers - 1) / concurrentPublishers;

            InnerPublisherSubscription.InnerPublisherSubscriptionBuilder<K, I, R> builder =
                  new InnerPublisherSubscription.InnerPublisherSubscriptionBuilder<>(this, targetBatchSize,
                        targetSupplier, excludedKeys, currentTopology);

            Publisher<E>[] publisherArray = new Publisher[concurrentPublishers];
            for (int i = 0; i < concurrentPublishers - 1; ++i) {
               publisherArray[i] = subToFlowableFunction.apply(builder, null);
            }
            // Submit the local target last if necessary (otherwise is a normal submission)
            // This is done last as we want to send all the remote requests first and only process the local
            // container concurrently with the remote requests
            if (localSegments != null) {
               publisherArray[concurrentPublishers - 1] = subToFlowableFunction.apply(builder, new AbstractMap.SimpleEntry<>(localAddress, localSegments));
            } else {
               publisherArray[concurrentPublishers - 1] = subToFlowableFunction.apply(builder, null);
            }

            return Flowable.mergeArray(publisherArray);
         }).repeatUntil(() -> {
            boolean complete = segmentsToComplete.isEmpty();
            if (log.isTraceEnabled()) {
               if (complete) {
                  log.tracef("All segments complete for %s", requestId);
               } else {
                  log.tracef("Segments %s not completed - retrying", segmentsToComplete);
               }
            }
            return complete;
         });
      }

      public Flowable<R> start() {
         return getValuesFlowable(InnerPublisherSubscription.InnerPublisherSubscriptionBuilder::createValuePublisher);
      }

      public Flowable<SegmentPublisherSupplier.Notification<R>> startWithSegments() {
         return getValuesFlowable(InnerPublisherSubscription.InnerPublisherSubscriptionBuilder::createNotificationPublisher);
      }

      void completeSegment(int segment) {
         segmentsToComplete.remove(segment);
         if (keysBySegment != null) {
            keysBySegment.set(segment, null);
         }
      }

      CompletionStage<PublisherResponse> sendInitialCommand(Address target, IntSet segments, int batchSize,
            Set<K> excludedKeys, int topologyId) {
         if (keysBySegment != null) {
            for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
               int segment = iter.nextInt();
               Collection<K> keys = keysBySegment.get(segment);
               if (keys != null) {
                  if (excludedKeys == null) {
                     excludedKeys = new HashSet<>();
                  }
                  excludedKeys.addAll(keys);
               }
            }
         }

         if (log.isTraceEnabled()) {
            log.tracef("Request: %s is initiating publisher request with batch size %d from %s in segments %s", requestId, batchSize,
                  target, segments);
         }
         boolean local = target == rpcManager.getAddress();
         InitialPublisherCommand cmd = publisher.buildInitialCommand(local ? null : target, requestId, segments, excludedKeys, batchSize,
               local && useContext.getAndSet(false));
         if (cmd == null) {
            return CompletableFuture.completedFuture(PublisherResponse.emptyResponse(segments, null));
         }
         // This means the target is local - so skip calling the rpcManager
         if (local) {
            try {
               return (CompletableFuture) cmd.invokeAsync(componentRegistry);
            } catch (Throwable throwable) {
               return CompletableFuture.failedFuture(throwable);
            }
         }
         cmd.setTopologyId(topologyId);

         return rpcManager.invokeCommand(target, cmd, SingleResponseCollector.validOnly(), rpcOptions)
               .thenApply(responseHandler);
      }

      CompletionStage<PublisherResponse> sendNextCommand(Address target, int topologyId) {
         if (log.isTraceEnabled()) {
            log.tracef("Request: %s is continuing publisher request from %s", requestId, target);
         }
         // Local command so just return the handler
         if (target == rpcManager.getAddress()) {
            return publisherHandler.getNext(requestId);
         }
         NextPublisherCommand cmd = publisher.buildNextCommand(requestId);

         cmd.setTopologyId(topologyId);
         return rpcManager.invokeCommand(target, cmd, SingleResponseCollector.validOnly(), rpcOptions)
               .thenApply(responseHandler);
      }

      /**
       * Handles logging the throwable and cancelling if necessary. Returns if publisher should continue processing or not.
       * If <b>false</b> is returned, it is expected that the caller propagates the {@link Throwable} instance, normally
       * via {@link Subscriber#onError(Throwable)}.
       */
      boolean handleThrowable(Throwable t, Address target, IntSet segments) {
         if (t instanceof SuspectException) {
            if (log.isTraceEnabled()) {
               log.tracef("Received suspect exception for id %s from node %s when requesting segments %s", requestId,
                     target, segments);
            }
            return true;
         }
         if (log.isTraceEnabled()) {
            log.tracef(t, "Received exception for id %s from node %s when requesting segments %s", requestId, target,
                  segments);
         }
         // Cancel out the command for the provided publisher - other should be cancelled
         sendCancelCommand(target);
         return false;
      }

      void sendCancelCommand(Address target) {
         CancelPublisherCommand command = commandsFactory.buildCancelPublisherCommand(requestId);
         CompletionStage<?> stage;
         if (target == rpcManager.getAddress()) {
            try {
               stage = command.invokeAsync(componentRegistry);
            } catch (Throwable throwable) {
               stage = CompletableFuture.failedFuture(throwable);
            }
         } else {
            stage = rpcManager.invokeCommand(target, command, VoidResponseCollector.ignoreLeavers(),
                  rpcOptions);
         }
         if (log.isTraceEnabled()) {
            stage.exceptionally(t -> {
               log.tracef("There was a problem cancelling publisher for id %s at address %s", requestId, target);
               return null;
            });
         }
      }

      @Override
      public void accept(I value, int segment) {
         if (keysBySegment != null) {
            Collection<K> keys = keysBySegment.get(segment);
            if (keys == null) {
               // It really is a Set, but we trust response has unique keys. Also ArrayList uses up less memory per
               // entry and resizes better than a Set and we don't know how many entries we may receive
               keys = new ArrayList<>();
               keysBySegment.set(segment, keys);
            }
            K key;
            // When tracking keys we always send back the key
            if (publisher.shouldTrackKeys) {
               key = (K) value;
            } else {
               // When keys are not tracked we return the key or entry as is and we have to convert if necessary (entry)
               key = publisher.composedType.toKey(value);
            }
            if (log.isTraceEnabled()) {
               log.tracef("Saving key %s for segment %d for id %s", Util.toStr(key), segment, requestId);
            }
            keys.add(key);
         }
      }
   }

   /**
    * Whether the response should contain the keys for the current non completed segment. Note that we currently
    * optimize the case where we know that we get back keys or entries without mapping to a new value. We only require
    * key tracking when delivery guarantee is EXACTLY_ONCE. In somes case we don't need to track keys if the transformer
    * is the identity function (delineated by being the same as {@link MarshallableFunctions#identity()} or the function
    * implements a special interface {@link ModifiedValueFunction} and it retains the original value.
    * @param deliveryGuarantee guarantee of the data
    * @param transformer provided transformer
    * @return should keys for the current segment be returned in the response
    */
   private static boolean shouldTrackKeys(DeliveryGuarantee deliveryGuarantee, Function<?, ?> transformer) {
      if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
         if (transformer == MarshallableFunctions.identity()) {
            return false;
         } else if (transformer instanceof ModifiedValueFunction) {
            return ((ModifiedValueFunction<?, ?>) transformer).isModified();
         }
         return true;
      }
      // EXACTLY_ONCE is the only mode where keys are tracked
      return false;
   }

   abstract class AbstractSegmentAwarePublisher<I, R> implements SegmentPublisherSupplier<R> {
      final ComposedType<K, I, R> composedType;
      final IntSet segments;
      final InvocationContext invocationContext;
      final long explicitFlags;
      final DeliveryGuarantee deliveryGuarantee;
      final int batchSize;
      final Function<? super Publisher<I>, ? extends Publisher<R>> transformer;
      final boolean shouldTrackKeys;

      // Prevents the context from being applied for every segment - only the first
      final AtomicBoolean usedContext = new AtomicBoolean();

      private AbstractSegmentAwarePublisher(ComposedType<K, I, R> composedType, IntSet segments, InvocationContext invocationContext,
            long explicitFlags, DeliveryGuarantee deliveryGuarantee, int batchSize,
            Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
         this.composedType = composedType;
         this.segments = segments != null ? segments : IntSets.immutableRangeSet(maxSegment);
         this.invocationContext = invocationContext;
         this.explicitFlags = explicitFlags;
         this.deliveryGuarantee = deliveryGuarantee;
         this.batchSize = batchSize;
         this.transformer = transformer;
         this.shouldTrackKeys = shouldTrackKeys(deliveryGuarantee, transformer);
      }

      public Publisher<Notification<R>> publisherWithSegments() {
         return new SubscriberHandler<I, R>(this, true).startWithSegments();
      }

      public Publisher<R> publisherWithoutSegments() {
         return new SubscriberHandler<I, R>(this, false).start();
      }

      abstract InitialPublisherCommand buildInitialCommand(Address target, String requestId, IntSet segments,
                                                           Set<K> excludedKeys, int batchSize, boolean useContext);

      NextPublisherCommand buildNextCommand(String requestId) {
         return commandsFactory.buildNextPublisherCommand(requestId);
      }
   }

   private class KeyAwarePublisherImpl<I, R> extends AbstractSegmentAwarePublisher<I, R> {
      final Set<K> keysToInclude;

      private KeyAwarePublisherImpl(Set<K> keysToInclude, ComposedType<K, I, R> composedType, IntSet segments,
            InvocationContext invocationContext, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            int batchSize, Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
         super(composedType, segments, invocationContext, explicitFlags, deliveryGuarantee, batchSize, transformer);
         this.keysToInclude = Objects.requireNonNull(keysToInclude);
      }

      Set<K> calculateKeysToUse(Set<K> keys, IntSet segments, Set<K> excludedKeys) {
         Set<K> results = null;
         for (K key : keys) {
            if ((excludedKeys == null || !excludedKeys.contains(key)) &&
                  segments.contains(keyPartitioner.getSegment(key))) {
               if (results == null) {
                  results = new HashSet<>();
               }
               results.add(key);
            }
         }
         return results;
      }

      @Override
      InitialPublisherCommand buildInitialCommand(Address target, String requestId, IntSet segments, Set<K> excludedKeys,
                                                  int batchSize, final boolean useContext) {
         Set<K> keysToUse = calculateKeysToUse(keysToInclude, segments, excludedKeys);
         if (keysToUse == null) {
            return null;
         }

         Function<? super Publisher<I>, ? extends Publisher<R>> functionToUse;
         int lookupEntryCount;
         if (useContext && invocationContext != null && (lookupEntryCount = invocationContext.lookedUpEntriesCount()) > 0) {
            // We have context values so we must prepend them to the publisher that is provided
            functionToUse = (SerializableFunction<Publisher<I>, Publisher<R>>) publisher -> {
               if (usedContext.getAndSet(true)) {
                  return transformer.apply(publisher);
               }
               List<I> contextValues = new ArrayList<>(lookupEntryCount);
               invocationContext.forEachValue((key, entry) -> {
                  if (keysToInclude.contains(key)) {
                     contextValues.add(composedType.fromCacheEntry(entry));
                  }
               });
               return transformer.apply(Flowable.concat(Flowable.fromIterable(contextValues), publisher));
            };
         } else {
            functionToUse = transformer;
         }

         DeliveryGuarantee guarantee = deliveryToUse(target, deliveryGuarantee, explicitFlags);

         return commandsFactory.buildInitialPublisherCommand(requestId, guarantee,
               batchSize, segments, keysToUse, excludedKeys, explicitFlags, composedType.isEntry(), shouldTrackKeys,
               functionToUse);
      }
   }

   private class SegmentAwarePublisherImpl<I, R> extends AbstractSegmentAwarePublisher<I, R> {
      private SegmentAwarePublisherImpl(IntSet segments, ComposedType<K, I, R> composedType,
            InvocationContext invocationContext, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            int batchSize, Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
         super(composedType, segments, invocationContext, explicitFlags, deliveryGuarantee, batchSize, transformer);
      }

      @Override
      InitialPublisherCommand buildInitialCommand(Address target, String requestId, IntSet segments, Set<K> excludedKeys,
                                                  int batchSize, boolean useContext) {
         Function<? super Publisher<I>, ? extends Publisher<R>> functionToUse;
         int lookupEntryCount;
         if (useContext && invocationContext != null && (lookupEntryCount = invocationContext.lookedUpEntriesCount()) > 0) {
            // We have context values so we must prepend them to the publisher that is provided
            functionToUse = (SerializableFunction<Publisher<I>, Publisher<R>>) publisher -> {
               if (usedContext.getAndSet(true)) {
                  return transformer.apply(publisher);
               }
               List<I> contextValues = new ArrayList<>(lookupEntryCount);
               invocationContext.forEachValue((key, entry) ->
                     contextValues.add(composedType.fromCacheEntry(entry)));
               return transformer.apply(Flowable.concat(Flowable.fromIterable(contextValues), publisher));
            };
         } else {
            functionToUse = transformer;
         }

         DeliveryGuarantee guarantee = deliveryToUse(target, deliveryGuarantee, explicitFlags);

         return commandsFactory.buildInitialPublisherCommand(requestId, guarantee,
               batchSize, segments, null, excludedKeys, explicitFlags, composedType.isEntry(), shouldTrackKeys, functionToUse);
      }
   }

   private DeliveryGuarantee deliveryToUse(Address target, DeliveryGuarantee desiredGuarantee, long explicitFlags) {
      // When the target is the local node and we have a shared store that doesn't have write behind we don't
      // need any special delivery guarantee as our store will hold all entries
      return target == null && skipRemoteInvocation(explicitFlags) ? DeliveryGuarantee.AT_MOST_ONCE : desiredGuarantee;
   }


   /**
    * Returns true if the publisher can do its thing without contacting the other nodes.
    * <p>
    * Replicated caches and Shared stores have all data locally. We do not need RPCs.
    * <p>
    * Consider {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD}.
    *
    * @param explicitFlags The flags used by the command.
    * @return {@code true} if it can perform everything locally.
    */
   private boolean skipRemoteInvocation(long explicitFlags) {
      // write behind may lose some data in remote nodes queues.
      return ((sharedStore && EnumUtil.noneOf(explicitFlags, FlagBitSets.SKIP_CACHE_LOAD)) || replicatedCache) && !writeBehindShared;
   }
}
