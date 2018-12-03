package org.infinispan.stream.impl;

import java.util.Collection;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Local stream manager implementation that handles injection of the stream supplier, invoking the operation and
 * subsequently notifying the operation if a rehash has changed one of its segments.
 * @param <Original> original stream type
 * @param <K> key type of underlying cache
 * @param <V> value type of underlying cache
 */
@Listener(observation = Listener.Observation.POST)
public class LocalStreamManagerImpl<Original, K, V> implements LocalStreamManager<Original, K> {
   private final static Log log = LogFactory.getLog(LocalStreamManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private AdvancedCache<K, V> cache;
   @Inject private ComponentRegistry registry;
   @Inject private DistributionManager dm;
   @Inject private RpcManager rpc;
   @Inject private CommandsFactory factory;
   @Inject private IteratorHandler iteratorHandler;
   private boolean hasLoader;
   private boolean isReplicated;
   private int maxSegment;

   private Address localAddress;

   private final ConcurrentMap<Object, SegmentListener> changeListener = CollectionFactory.makeConcurrentMap();
   private ByteString cacheName;

   class SegmentListener {
      private final IntSet segments;
      private final SegmentAwareOperation op;
      private final IntSet segmentsLost;

      SegmentListener(IntSet segments, SegmentAwareOperation op) {
         // Need to make a copy since segments may be modified by user after
         this.segments = IntSets.mutableCopyFrom(segments);
         this.op = op;
         this.segmentsLost = IntSets.concurrentSet(maxSegment);
      }

      public void localSegments(IntSet localSegments) {
         for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
            int segment = segmentIterator.nextInt();
            if (!localSegments.contains(segment)) {
               if (trace) {
                  log.tracef("Could not process segment %s", segment);
               }
               segmentsLost.add(segment);
            }
         }
      }

      public void lostSegments(IntSet lostSegments) {
         for (PrimitiveIterator.OfInt segmentIterator = lostSegments.iterator(); segmentIterator.hasNext(); ) {
            int segment = segmentIterator.nextInt();
            if (segments.contains(segment)) {
               if (trace) {
                  log.tracef("Lost segment %s", segment);
               }
               if (op.lostSegment(false)) {
                  if (segmentsLost.add(segment) && segmentsLost.size() == segments.size()) {
                     if (trace) {
                        log.tracef("All segments %s are now lost", segments);
                     }
                     op.lostSegment(true);
                  }
               }
            }
         }
      }
   }

   /**
    * Injects the cache - unfortunately this cannot be in start. Tests will rewire certain components which will in
    * turn reinject the cache, but they won't call the start method! If the latter is fixed we can add this to start
    * method and add @Inject to the variable.
    * @param cache
    */
   @Inject
   public void inject(Cache<K, V> cache) {
      // We need to unwrap the cache as a local stream should only deal with BOXED values and obviously only
      // with local entries.  Any mappings will be provided by the originator node in their intermediate operation
      // stack in the operation itself.
      // Also this class manages iterations that were called for remotely so make sure our commands know this
      this.cache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL,
            Flag.REMOTE_ITERATION);
   }

   @Start
   public void start() {
      cacheName = ByteString.fromString(cache.getName());
      hasLoader = cache.getCacheConfiguration().persistence().usingStores();
      ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
      this.maxSegment = clusteringConfiguration.hash().numSegments();
      localAddress = rpc.getAddress();
      isReplicated = clusteringConfiguration.cacheMode().isReplicated();
      // Replicated never removes segments so we don't have to worry about rehash event
      if (!isReplicated) {
         cache.addListener(this);
      }
   }

   /**
    * We need to listen to data rehash events in case if data moves while we are iterating over it.
    * If a rehash occurs causing this node to lose a segment and there is something iterating over the stream looking
    * for values of that segment, we can't guarantee that the data has all been seen correctly, so we must therefore
    * suspect that node by sending it back to the owner.
    * @param event The data rehash event
    */
   @DataRehashed
   public void dataRehashed(DataRehashedEvent<K, V> event) {
      ConsistentHash startHash = event.getConsistentHashAtStart();
      ConsistentHash endHash = event.getConsistentHashAtEnd();
      boolean trace = log.isTraceEnabled();
      if (startHash != null && endHash != null) {
         if (trace) {
            log.tracef("Data rehash occurred startHash: %s and endHash: %s with new topology %s and was pre %s", startHash, endHash,
                    event.getNewTopologyId(), event.isPre());
         }

         if (!changeListener.isEmpty()) {
            if (trace) {
               log.tracef("Previous segments %s ", startHash.getSegmentsForOwner(localAddress));
               log.tracef("After segments %s ", endHash.getSegmentsForOwner(localAddress));
            }
            // we don't care about newly added segments, since that means our run wouldn't include them anyways
            IntSet beforeSegments = IntSets.mutableFrom(startHash.getSegmentsForOwner(localAddress));
            // Now any that were there before but aren't there now should be added - we don't care about new segments
            // since our current request shouldn't be working on it - it will have to retrieve it later
            beforeSegments.removeAll(endHash.getSegmentsForOwner(localAddress));
            if (!beforeSegments.isEmpty()) {
               // We have to make sure all current listeners get the newest hashes updated.  This has to occur for
               // new nodes and nodes leaving as the hash segments will change in both cases.
               for (Map.Entry<Object, SegmentListener> entry : changeListener.entrySet()) {
                  if (trace) {
                     log.tracef("Notifying %s through SegmentChangeListener", entry.getKey());
                  }
                  entry.getValue().lostSegments(beforeSegments);
               }
            } else if (trace) {
               log.tracef("No segments have been removed from data rehash, no notification required");
            }
         } else if (trace) {
            log.tracef("No change listeners present!");
         }
      }
   }

   private AdvancedCache<K, V> getCacheRespectingLoader(boolean includeLoader) {
      // We only need to "skip" the loader if there is one and we were told to skip it
      if (hasLoader && !includeLoader) {
         return cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD);
      }
      return cache;
   }

   private CacheSet<Original> toOriginalSet(boolean includeLoader, boolean entryStream) {
      if (entryStream) {
         return (CacheSet<Original>) getCacheRespectingLoader(includeLoader).cacheEntrySet();
      } else {
         return (CacheSet<Original>) getCacheRespectingLoader(includeLoader).keySet();
      }
   }

   private Stream<Original> getStream(CacheSet<Original> cacheEntrySet, boolean parallelStream,
         boolean entryStream, IntSet segments, Set<K> keysToInclude, Set<K> keysToExclude) {
      Stream<Original> stream = (parallelStream ? cacheEntrySet.parallelStream() : cacheEntrySet.stream())
              .filterKeys(keysToInclude).filterKeySegments(segments);
      if (!keysToExclude.isEmpty()) {
         if (entryStream) {
            // If it is an entry stream we have to exclude by key
            return stream.filter(e -> !keysToExclude.contains(((CacheEntry) e).getKey()));
         } else {
            return stream.filter(k -> !keysToExclude.contains(k));
         }
      }
      return stream;
   }

   private Stream<Original> getRehashStream(CacheSet<Original> cacheEntrySet, Object requestId,
         SegmentListener listener, boolean parallelStream, boolean entryStream, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude) {
      if (listener != null) {
         handleSuspectSegmentsBeforeStream(requestId, listener, segments);
      }

      return getStream(cacheEntrySet, parallelStream, entryStream, segments, keysToInclude, keysToExclude);
   }

   // We should first handle any segments we already lost before processing. Remove from {@code segments} all the
   // segments for which this node is no longer an owner add them to {@code listener.segmentsLost}. Note this means
   // that we allow backup owners to respond even though it was thought to be a primary owner.
   private void handleSuspectSegmentsBeforeStream(Object requestId, SegmentListener listener, IntSet segments) {
      LocalizedCacheTopology topology = dm.getCacheTopology();
      if (trace) {
         log.tracef("Topology for supplier is %s for id %s", topology, requestId);
      }
      ConsistentHash readCH = topology.getCurrentCH();
      ConsistentHash pendingCH = topology.getPendingCH();
      if (pendingCH != null) {
         IntSet lostSegments = IntSets.mutableEmptySet(topology.getCurrentCH().getNumSegments());
         PrimitiveIterator.OfInt iterator = segments.iterator();
         while (iterator.hasNext()) {
            int segment = iterator.nextInt();
            // If the segment is not owned by both CHs we can't use it during rehash
            if (!pendingCH.isSegmentLocalToNode(localAddress, segment)
                  || !readCH.isSegmentLocalToNode(localAddress, segment)) {
               iterator.remove();
               lostSegments.set(segment);
            }
         }
         if (!lostSegments.isEmpty()) {
            if (trace) {
               log.tracef("Lost segments %s during rehash for id %s", lostSegments, requestId);
            }
            listener.lostSegments(lostSegments);
         } else if (trace) {
            log.tracef("Currently in the middle of a rehash for id %s", requestId);
         }
      } else {
         IntSet ourSegments = topology.getLocalReadSegments();
         if (segments.retainAll(ourSegments)) {
            if (trace) {
               log.tracef("We found to be missing some segments requested for id %s", requestId);
            }
            listener.localSegments(ourSegments);
         } else if (trace) {
            log.tracef("Hash %s for id %s", readCH, requestId);
         }
      }
   }

   private void handleResponseError(CompletionStage<ValidResponse> rpcFuture, Object requestId,
                                    Address origin) {
      if (trace) {
         rpcFuture.whenComplete((response, e) -> {
            if (e != null) {
               log.tracef(e, "Encountered exception for %s sending response to %s", requestId, origin);
            } else if (response != null && !response.isSuccessful()) {
               log.tracef("Unsuccessful response for %s sending response to %s", requestId, origin);
            } else {
               log.tracef("Response successfully sent for %s", requestId);
            }
         });
      }
   }

   @Override
   public <R> void streamOperation(Object requestId, Address origin, boolean parallelStream, IntSet segments,
         Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
         TerminalOperation<Original, R> operation) {
      if (trace) {
         log.tracef("Received operation request for id %s from %s for segments %s", requestId, origin, segments);
      }
      CacheSet<Original> originalSet = toOriginalSet(includeLoader, entryStream);
      operation.setSupplier(() -> getStream(originalSet, parallelStream, entryStream, segments, keysToInclude,
            keysToExclude));
      operation.handleInjection(registry);
      R value = operation.performOperation();
      StreamResponseCommand<R> command =
         factory.buildStreamResponseCommand(requestId, true, IntSets.immutableEmptySet(), value);
      CompletionStage<ValidResponse> completableFuture =
         rpc.invokeCommand(origin, command, SingleResponseCollector.validOnly(), rpc.getSyncRpcOptions());
      handleResponseError(completableFuture, requestId, origin);
   }

   @Override
   public <R> void streamOperationRehashAware(Object requestId, Address origin, boolean parallelStream,
           IntSet segments, Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
           TerminalOperation<Original, R> operation) {
      if (trace) {
         log.tracef("Received rehash aware operation request for id %s from %s for segments %s", requestId, origin,
                 segments);
      }
      CacheSet<Original> originalSet = toOriginalSet(includeLoader, entryStream);
      R value;

      operation.handleInjection(registry);
      SegmentListener listener;
      if (isReplicated) {
         listener = null;
      } else {
         listener = new SegmentListener(segments, operation);
         // We currently only allow 1 request per id (we may change this later)
         changeListener.put(requestId, listener);
         if (trace) {
            log.tracef("Registered change listener for %s", requestId);
         }
      }
      IntSet lostSegments;
      try {
         operation.setSupplier(() -> getRehashStream(originalSet, requestId, listener, parallelStream, entryStream,
               segments, keysToInclude, keysToExclude));
         value = operation.performOperation();
         if (listener != null) {
            lostSegments = listener.segmentsLost;
         } else {
            lostSegments = IntSets.immutableEmptySet();
         }
         if (trace) {
            log.tracef("Request %s completed for segments %s with %s suspected segments", requestId, segments,
                    lostSegments);
         }
      } finally {
         if (listener != null) {
            changeListener.remove(requestId);
            if (trace) {
               log.tracef("UnRegistered change listener for %s", requestId);
            }
         }
      }
      sendRehashAwareResponse(requestId, origin, lostSegments, segments, value);
   }

   @Override
   public <R> void streamOperation(Object requestId, Address origin, boolean parallelStream, IntSet segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
           KeyTrackingTerminalOperation<Original, K, R> operation) {
      if (trace) {
         log.tracef("Received key aware operation request for id %s from %s for segments %s", requestId, origin,
                 segments);
      }
      CacheSet<Original> originalSet = toOriginalSet(includeLoader, entryStream);
      operation.setSupplier(() -> getStream(originalSet, parallelStream, entryStream, segments, keysToInclude,
            keysToExclude));
      operation.handleInjection(registry);
      Collection<R> value = operation.performOperation(new NonRehashIntermediateCollector<>(origin, requestId,
              parallelStream));
      StreamResponseCommand<Collection<R>> command =
         factory.buildStreamResponseCommand(requestId, true, IntSets.immutableEmptySet(), value);
      CompletionStage<ValidResponse> completableFuture =
         rpc.invokeCommand(origin, command, SingleResponseCollector.validOnly(), rpc.getSyncRpcOptions());
      handleResponseError(completableFuture, requestId, origin);
   }

   @Override
   public void streamOperationRehashAware(Object requestId, Address origin, boolean parallelStream,
         IntSet segments, Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
         KeyTrackingTerminalOperation<Original, K, ?> operation) {
      if (trace) {
         log.tracef("Received key rehash aware operation request for id %s from %s for segments %s", requestId, origin,
                 segments);
      }
      CacheSet<Original> originalSet = toOriginalSet(includeLoader, entryStream);
      Collection<K> results;

      operation.handleInjection(registry);
      SegmentListener listener;
      if (isReplicated) {
         listener = null;
      } else {
         listener = new SegmentListener(segments, operation);
         // We currently only allow 1 request per id (we may change this later)
         changeListener.put(requestId, listener);
         if (trace) {
            log.tracef("Registered change listener for %s", requestId);
         }
      }
      IntSet lostSegments;
      try {
         operation.setSupplier(() -> getRehashStream(originalSet, requestId, listener, parallelStream, entryStream,
               segments, keysToInclude, keysToExclude));
         results = operation.performForEachOperation(new NonRehashIntermediateCollector<>(origin, requestId,
                 parallelStream));
         if (listener != null) {
            lostSegments = listener.segmentsLost;
         } else {
            lostSegments = IntSets.immutableEmptySet();
         }
         if (trace) {
            log.tracef("Request %s completed segments %s with %s suspected segments", requestId, segments, lostSegments);
         }
      } finally {
         if (listener != null) {
            changeListener.remove(requestId);
            if (trace) {
               log.tracef("UnRegistered change listener for %s", requestId);
            }
         }
      }
      sendRehashAwareResponse(requestId, origin, lostSegments, segments, results);
   }

   private <R> void sendRehashAwareResponse(Object requestId, Address origin, IntSet lostSegments,
         IntSet originalSegments, R value) {
      // We can only send a response if the cache is still running or starting up
      // Otherwise we send no response and rely on the fact that the command originator will get a SuspectException
      // or equivalent.
      if (cache.getStatus() != ComponentStatus.RUNNING && cache.getStatus() != ComponentStatus.INITIALIZING) {
         if (lostSegments.isEmpty()) {
            lostSegments = originalSegments;
         } else {
            lostSegments.addAll(originalSegments);
         }
         if (trace) {
            log.tracef("Cache status is no longer running, all segments are now suspect for %s", requestId);
         }
         value = null;
      }
      StreamResponseCommand<R> command = factory.buildStreamResponseCommand(requestId, true, lostSegments, value);
      CompletionStage<ValidResponse> completableFuture =
            this.rpc.invokeCommand(origin, command, SingleResponseCollector.validOnly(), rpc.getSyncRpcOptions());
      handleResponseError(completableFuture, requestId, origin);
   }

   @Override
   public IteratorResponse startIterator(Object requestId, Address origin, IntSet segments, Set<K> keysToInclude,
         Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
         Iterable<IntermediateOperation> intermediateOperations, long batchSize) {
      if (trace) {
         log.tracef("Received rehash aware operation request to start iterator for id %s from %s for segments %s",
               requestId, origin, segments);
      }
      CacheSet<Original> originalSet = toOriginalSet(includeLoader, entryStream);

      // Normally we wouldn't use a listener to improve performance in REPL - but we need it for iterator to communicate
      // to the response command that segments can be lost if cache is no longer running via the below Runnable
      SegmentListener listener = new SegmentListener(segments, i -> true);
      Runnable listenerClosedRunnable = () -> {
         changeListener.remove(requestId);
         // If the cache is no longer running, all segments have to be suspect
         if (cache.getStatus() != ComponentStatus.RUNNING && cache.getStatus() != ComponentStatus.INITIALIZING) {
            if (trace) {
               log.tracef("Cache status is no longer running after completing iterator, all segments are now suspect for %s", requestId);
            }
            listener.segmentsLost.addAll(segments);
         }
      };

      if (changeListener.putIfAbsent(requestId, listener) != null) {
         throw new IllegalStateException("Iterator was already created for id " + requestId);
      }

      if (trace) {
         log.tracef("Registered change listener for %s", requestId);
      }
      IteratorHandler.OnCloseIterator<Object> iterator = iteratorHandler.start(origin, () -> getRehashStream(originalSet,
            requestId, isReplicated ? null : listener, false, entryStream, segments, keysToInclude, keysToExclude),
            intermediateOperations, requestId);
      // Have to clear out our change listener when the iterator is closed
      iterator.onClose(listenerClosedRunnable);

      return new IteratorResponses.RemoteResponse(iterator, listener.segmentsLost, batchSize);
   }

   @Override
   public IteratorResponse continueIterator(Object requestId, long batchSize) {
      CloseableIterator<Object> iterator = iteratorHandler.getIterator(requestId);
      return new IteratorResponses.RemoteResponse(iterator, changeListener.get(requestId).segmentsLost, batchSize);
   }

   class NonRehashIntermediateCollector<R> implements KeyTrackingTerminalOperation.IntermediateCollector<R> {
      private final Address origin;
      private final Object requestId;
      private final boolean useManagedBlocker;

      NonRehashIntermediateCollector(Address origin, Object requestId, boolean useManagedBlocker) {
         this.origin = origin;
         this.requestId = requestId;
         this.useManagedBlocker = useManagedBlocker;
      }

      @Override
      public void sendDataResonse(R response) {
         // If we know we were in a parallel stream we should use a managed blocker to not consume core fork join
         // threads if applicable.
         if (useManagedBlocker) {
            try {
               // We use a managed blocker just in case if this invoked in the common thread pool
               ForkJoinPool.managedBlock(new ResponseBlocker(response));
            } catch (InterruptedException e) {
               throw new CacheException(e);
            }
         } else {
            StreamResponseCommand<R> command =
               new StreamResponseCommand<>(cacheName, localAddress, requestId, false, response);
            rpc.blocking(
               rpc.invokeCommand(origin, command, SingleResponseCollector.validOnly(), rpc.getSyncRpcOptions()));
         }
      }

      class ResponseBlocker implements ForkJoinPool.ManagedBlocker {
         private final R response;
         private boolean completed = false;

         ResponseBlocker(R response) {
            this.response = response;
         }

         @Override
         public boolean block() throws InterruptedException {
            if (!completed) {
               // This way we don't send more than 1 response to the originating node but still inside managed blocker
               // so we don't consume a thread
               synchronized (NonRehashIntermediateCollector.this) {
                  StreamResponseCommand<R> command = new StreamResponseCommand<>(cacheName, localAddress,
                                                                             requestId, false, response);
                  rpc.blocking(
                     rpc.invokeCommand(origin, command, SingleResponseCollector.validOnly(), rpc.getSyncRpcOptions()));
               }
            }
            completed = true;
            return completed;
         }

         @Override
         public boolean isReleasable() {
            return completed;
         }
      }
   }
}
