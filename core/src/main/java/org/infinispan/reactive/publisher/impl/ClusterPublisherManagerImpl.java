package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.PublishProcessor;

/**
 * ClusterPublisherManager that determines targets for the given segments and/or keys and then sends to local and
 * remote nodes in parallel collecting results to be returned. This implement prioritizes running as much as possible
 * on the local node, in some cases not even going remotely if all keys or segments are available locally.
 * @author wburns
 * @since 10.0
 */
public class ClusterPublisherManagerImpl<K, V> implements ClusterPublisherManager<K, V> {
   protected final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected final static boolean trace = log.isTraceEnabled();

   @Inject private LocalPublisherManager<K, V> localPublisherManager;
   @Inject private DistributionManager distributionManager;
   @Inject private StateTransferLock stateTransferLock;
   @Inject private RpcManager rpcManager;
   @Inject private CommandsFactory commandsFactory;
   @Inject protected Configuration cacheConfiguration;

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

   private int maxSegment;
   private boolean writeBehindShared;

   @Start
   public void start() {
      maxSegment = distributionManager.getReadConsistentHash().getNumSegments();
      writeBehindShared = hasWriteBehindSharedStore(cacheConfiguration.persistence());
   }

   @Override
   public <R> CompletionStage<R> keyReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      // Needs to be serialized processor as we can write to it from different threads
      FlowableProcessor<R> flowableProcessor = PublishProcessor.<R>create().toSerialized();
      // We apply the finalizer first to ensure they can subscribe to the PublishProcessor before we emit any items
      CompletionStage<R> stage = finalizer.apply(flowableProcessor);

      Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizerToUse =
            requiresFinalizer(parallelPublisher, keysToInclude, deliveryGuarantee) ? finalizer : null;

      if (keysToInclude != null) {
         startKeyPublisher(parallelPublisher, segments, keysToInclude, keysToExclude, includeLoader, deliveryGuarantee,
               keyComposedType(), transformer, finalizerToUse, flowableProcessor);
      } else {
         startSegmentPublisher(parallelPublisher, segments, keysToExclude, includeLoader,
               deliveryGuarantee, keyComposedType(), transformer, finalizerToUse, flowableProcessor);
      }
      return stage;
   }

   @Override
   public <R> CompletionStage<R> entryReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      // Needs to be serialized processor as we can write to it from different threads
      FlowableProcessor<R> flowableProcessor = PublishProcessor.<R>create().toSerialized();
      // We apply the finalizer first to ensure they can subscribe to the PublishProcessor before we emit any items
      CompletionStage<R> stage = finalizer.apply(flowableProcessor);

      Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizerToUse =
            requiresFinalizer(parallelPublisher, keysToInclude, deliveryGuarantee) ? finalizer : null;

      if (keysToInclude != null) {
         startKeyPublisher(parallelPublisher, segments, keysToInclude, keysToExclude, includeLoader, deliveryGuarantee,
               entryComposedType(), transformer, finalizerToUse, flowableProcessor);
      } else {
         startSegmentPublisher(parallelPublisher, segments, keysToExclude, includeLoader,
               deliveryGuarantee, entryComposedType(), transformer, finalizerToUse, flowableProcessor);
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

   private <I, R> void startKeyPublisher(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude, Set<K> keysToExclude,
         boolean includeLoader, DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer,
         FlowableProcessor<R> flowableProcessor) {
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      Address localAddress = topology.getLocalAddress();
      Map<Address, Set<K>> keyTargets = determineKeyTargets(topology, keysToInclude, localAddress, segments, keysToExclude);

      AtomicInteger parallelCount = new AtomicInteger(keyTargets.size());

      // This way we only have to allocate 1 per request chain
      BiConsumer<PublisherResult<R>, Throwable> biConsumer = new KeyBiConsumer<>(flowableProcessor,
            parallelCount, topology.getTopologyId(), parallelPublisher, includeLoader, deliveryGuarantee,
            composedType, transformer, finalizer);

      Set<K> localKeys = keyTargets.remove(localAddress);
      // If any targets left, they are all remote
      if (!keyTargets.isEmpty()) {
         // We submit the remote ones first as they will not block at all, just to send remote tasks
         for (Map.Entry<Address, Set<K>> remoteTarget : keyTargets.entrySet()) {
            Set<K> remoteKeys = remoteTarget.getValue();
            PublisherRequestCommand<K> command = composedType.remoteInvocation(parallelPublisher, null, remoteKeys,
                  keysToExclude, includeLoader, deliveryGuarantee, transformer, finalizer);
            command.setTopologyId(topology.getTopologyId());
            Address remoteAddress = remoteTarget.getKey();
            CompletionStage<PublisherResult<R>> stage = rpcManager.invokeCommand(remoteAddress, command,
                  new KeyPublisherResultCollector<>(remoteKeys), rpcManager.getSyncRpcOptions());
            stage.whenComplete(biConsumer);
         }
      }

      if (localKeys != null) {
         CompletionStage<PublisherResult<R>> localStage = composedType.localInvocation(parallelPublisher, null,
               localKeys, keysToExclude, includeLoader, deliveryGuarantee, transformer, finalizer);

         if (trace) {
            // Make sure the trace occurs before response is processed
            localStage = localStage.whenComplete((results, t) ->
                  log.tracef("Result result was: %s for keys %s from %s with %s suspected segments",
                        results.getResult(), localKeys, localAddress, results.getSuspectedSegments())
            );
         }

         // Map to the same collector, so we can reuse the same BiConsumer
         localStage.whenComplete(biConsumer);
      }
   }

   private <I, R> void startSegmentPublisher(boolean parallelPublisher, IntSet segments,
         Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer,
         FlowableProcessor<R> flowableProcessor) {
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      Address localAddress = topology.getLocalAddress();
      Map<Address, IntSet> targets = determineSegmentTargets(topology, segments, localAddress);

      // used to determine that last parallel completion, to either complete or retry
      AtomicInteger parallelCount = new AtomicInteger(targets.size());

      IntSet localSegments = targets.remove(localAddress);

      // This way we only have to allocate 1 per request chain
      BiConsumer<PublisherResult<R>, Throwable> biConsumer = new SegmentSpecificConsumer<>(flowableProcessor,
            parallelCount, topology.getTopologyId(), parallelPublisher, keysToExclude, includeLoader, deliveryGuarantee,
            composedType, transformer, finalizer);

      // If any targets left, they are all remote
      if (!targets.isEmpty()) {
         // We submit the remote ones first as they will not block at all, just to send remote tasks
         for (Map.Entry<Address, IntSet> remoteTarget : targets.entrySet()) {
            IntSet remoteSegments = remoteTarget.getValue();
            PublisherRequestCommand<K> command = composedType.remoteInvocation(parallelPublisher, remoteSegments, null,
                  keysToExclude, includeLoader, deliveryGuarantee, transformer, finalizer);
            command.setTopologyId(topology.getTopologyId());
            Address remoteAddress = remoteTarget.getKey();
            CompletionStage<PublisherResult<R>> stage = rpcManager.invokeCommand(remoteAddress, command,
                  new SegmentPublisherResultCollector<>(remoteSegments), rpcManager.getSyncRpcOptions());
            stage.whenComplete(biConsumer);
         }
      }

      if (localSegments != null) {
         CompletionStage<PublisherResult<R>> localStage = composedType.localInvocation(parallelPublisher, localSegments,
               null, keysToExclude, includeLoader, deliveryGuarantee, transformer, finalizer);

         if (trace) {
            // Make sure the trace occurs before response is processed
            localStage = localStage.whenComplete((results, t) ->
                  log.tracef("Result result was: %s for segments %s from %s with %s suspected segments",
                        results.getResult(), localSegments, localAddress, results.getSuspectedSegments())
            );
         }

         // Map to the same collector, so we can reuse the same BiConsumer
         localStage.whenComplete(biConsumer);
      }
   }

   private class SegmentSpecificConsumer<I, R> implements BiConsumer<PublisherResult<R>, Throwable> {
      private final FlowableProcessor<R> flowableProcessor;
      private final AtomicInteger parallelCount;

      private final IntSet segmentsToRetry = IntSets.concurrentSet(maxSegment);

      private final int currentTopologyId;
      private final boolean parallelPublisher;
      private final Set<K> keysToExclude;
      private final boolean includeLoader;
      private final DeliveryGuarantee deliveryGuarantee;
      private final ComposedType<K, I, R> composedType;
      private final Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer;
      private final Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer;

      SegmentSpecificConsumer(FlowableProcessor<R> flowableProcessor, AtomicInteger parallelCount,
            int currentTopologyId, boolean parallelPublisher, Set<K> keysToExclude, boolean includeLoader,
            DeliveryGuarantee deliveryGuarantee, ComposedType<K, I, R> composedType,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         this.flowableProcessor = flowableProcessor;
         this.parallelCount = parallelCount;

         this.currentTopologyId = currentTopologyId;
         this.parallelPublisher = parallelPublisher;
         this.keysToExclude = keysToExclude;
         this.includeLoader = includeLoader;
         this.deliveryGuarantee = deliveryGuarantee;
         this.composedType = composedType;
         this.transformer = transformer;
         this.finalizer = finalizer;
      }

      @Override
      public void accept(PublisherResult<R> resultCollector, Throwable t) {
         if (t != null) {
            if (trace) {
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
            if (trace) {
               log.tracef("Retrying segments %s after %d is installed", segmentsToRetry, nextTopology);
            }
            // If we had an issue with segments, we need to wait until the next topology is installed to try again
            stateTransferLock.topologyFuture(nextTopology).whenComplete((ign, innerT) -> {
               if (innerT != null) {
                  if (trace) {
                     log.tracef(innerT, "General error encountered when waiting on topology future for publisher request command");
                  }
                  flowableProcessor.onError(innerT);
               } else {
                  // Restart with the missing segments
                  startSegmentPublisher(parallelPublisher, segmentsToRetry, keysToExclude, includeLoader, deliveryGuarantee,
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
      private final boolean includeLoader;
      private final DeliveryGuarantee deliveryGuarantee;
      private final ComposedType<K, I, R> composedType;
      private final Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer;
      private final Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer;

      KeyBiConsumer(FlowableProcessor<R> flowableProcessor, AtomicInteger parallelCount, int currentTopologyId,
            boolean parallelPublisher, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            ComposedType<K, I, R> composedType, Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         this.flowableProcessor = flowableProcessor;
         this.parallelCount = parallelCount;
         this.currentTopologyId = currentTopologyId;
         this.parallelPublisher = parallelPublisher;
         this.includeLoader = includeLoader;
         this.deliveryGuarantee = deliveryGuarantee;
         this.composedType = composedType;
         this.transformer = transformer;
         this.finalizer = finalizer;
      }

      @Override
      public void accept(PublisherResult<R> resultCollector, Throwable t) {
         if (t != null) {
            if (trace) {
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
         R actualValue = result.getResult();
         if (actualValue != null) {
            flowableProcessor.onNext(actualValue);
         } else {
            // If there wasn't a result it means there was some suspected keys most likely
            keysToRetry.addAll((Set) result.getSuspectedKeys());
         }
      }

      private void onCompletion() {
         if (keysToRetry.isEmpty()) {
            flowableProcessor.onComplete();
         } else {
            int nextTopology = currentTopologyId + 1;
            if (trace) {
               log.tracef("Retrying keys %s after %d is installed", keysToRetry, nextTopology);
            }
            // If we had an issue with segments, we need to wait until the next topology is installed to try again
            stateTransferLock.topologyFuture(nextTopology).whenComplete((ign, innerT) -> {
               if (innerT != null) {
                  if (trace) {
                     log.tracef(innerT, "General error encountered when waiting on topology future for publisher request command");
                  }
                  flowableProcessor.onError(innerT);
               } else {
                  // Restart with keys that were missing - note that segments and exclude is always null - as we
                  // already filtered those out in the first startKeyPublisher invocation
                  startKeyPublisher(parallelPublisher, null, keysToRetry, null, includeLoader, deliveryGuarantee,
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
         PublisherResult<R> results = (PublisherResult<R>) response.getResponseValue();
         if (trace) {
            log.tracef("Result result was: %s for keys %s from %s", results.getResult(), keys, sender);
         }
         return results;
      }

      @Override
      protected PublisherResult<R> addTargetNotFound(Address sender) {
         if (trace) {
            log.tracef("Cache is no longer running for keys %s from %s - must retry", Util.toStr(keys), sender);
         }
         return new KeyPublisherResult<>(keys);
      }

      @Override
      protected PublisherResult<R> addException(Address sender, Exception exception) {
         if (trace) {
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
         PublisherResult<R> results = (PublisherResult<R>) response.getResponseValue();
         if (trace) {
            log.tracef("Result result was: %s for segments %s from %s with %s suspected segments", results.getResult(),
                  targetSegments, sender, results.getSuspectedSegments());
         }
         return results;
      }

      @Override
      protected PublisherResult<R> addTargetNotFound(Address sender) {
         if (trace) {
            log.tracef("Cache is no longer running for segments %s from %s - must retry", targetSegments, sender);
         }
         return new SegmentPublisherResult<>(targetSegments, null);
      }

      @Override
      protected PublisherResult<R> addException(Address sender, Exception exception) {
         if (trace) {
            log.tracef(exception, "Exception encountered while requesting segments %s from %s", targetSegments, sender);
         }
         // Throw the exception so it is propagated to caller
         if (exception instanceof CacheException) {
            throw (CacheException) exception;
         }
         throw new CacheException(exception);
      }
   }

   private Map<Address, IntSet> determineSegmentTargets(LocalizedCacheTopology topology, IntSet segments, Address localAddress) {
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
      return targets;
   }

   private void handleSegment(int segment, LocalizedCacheTopology topology, Address localAddress,
         Map<Address, IntSet> targets) {
      Address owner;
      DistributionInfo distributionInfo = topology.getSegmentDistribution(segment);
      // Prioritize local node even if it is backup when we don't have a shared write behind store
      if (!writeBehindShared && distributionInfo.isReadOwner()) {
         owner = localAddress;
      } else {
         owner = distributionInfo.primary();
      }
      addToMap(targets, owner, segment);
   }

   private void addToMap(Map<Address, IntSet> map, Address owner, int segment) {
      IntSet set = map.get(owner);
      if (set == null) {
         set = IntSets.mutableEmptySet();
         map.put(owner, set);
      }
      set.set(segment);
   }

   private Map<Address, Set<K>> determineKeyTargets(LocalizedCacheTopology topology, Set<K> keys, Address localAddress,
         IntSet segments, Set<K> keysToExclude) {
      Map<Address, Set<K>> filteredKeys = null;
      if (keys != null) {
         filteredKeys = new HashMap<>();
         for (K key : keys) {
            if (keysToExclude != null && keysToExclude.contains(key)) {
               continue;
            }
            DistributionInfo distributionInfo = topology.getDistribution(key);
            if (segments != null && !segments.contains(distributionInfo.segmentId())) {
               continue;
            }
            Address targetAddress;
            if (!writeBehindShared && distributionInfo.isReadOwner()) {
               targetAddress = localAddress;
            } else {
               targetAddress = distributionInfo.primary();
            }
            addToMap(filteredKeys, targetAddress, key);
         }
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
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);

      PublisherRequestCommand<K> remoteInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer);
   }

   private class KeyComposedType<R> implements ComposedType<K, K, R> {

      @Override
      public CompletionStage<PublisherResult<R>> localInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return localPublisherManager.keyReduction(parallelPublisher, segments, keysToInclude, keysToExclude,
               includeLoader, deliveryGuarantee, transformer, finalizer);
      }

      @Override
      public PublisherRequestCommand<K> remoteInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return commandsFactory.buildKeyPublisherCommand(parallelPublisher, deliveryGuarantee, segments, keysToInclude,
               keysToExclude, includeLoader, transformer, finalizer);
      }
   }

   private class EntryComposedType<R> implements ComposedType<K, CacheEntry<K, V>, R> {

      @Override
      public CompletionStage<PublisherResult<R>> localInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return localPublisherManager.entryReduction(parallelPublisher, segments, keysToInclude, keysToExclude,
               includeLoader, deliveryGuarantee, transformer, finalizer);
      }

      @Override
      public PublisherRequestCommand<K> remoteInvocation(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            Set<K> keysToExclude, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return commandsFactory.buildEntryPublisherCommand(parallelPublisher, deliveryGuarantee, segments, keysToInclude,
               keysToExclude, includeLoader, transformer, finalizer);
      }
   }

   private boolean hasWriteBehindSharedStore(PersistenceConfiguration persistenceConfiguration) {
      for (StoreConfiguration storeConfiguration : persistenceConfiguration.stores()) {
         if (storeConfiguration.shared() && storeConfiguration.async().enabled()) {
            return true;
         }
      }
      return false;
   }
}
