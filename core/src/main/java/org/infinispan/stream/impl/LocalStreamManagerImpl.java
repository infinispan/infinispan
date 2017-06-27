package org.infinispan.stream.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Local stream manager implementation that handles injection of the stream supplier, invoking the operation and
 * subsequently notifying the operation if a rehash has changed one of its segments.
 * @param <K> key type of underlying cache
 * @param <V> value type of underlying cache
 */
@Listener(observation = Listener.Observation.POST)
public class LocalStreamManagerImpl<K, V> implements LocalStreamManager<K> {
   private final static Log log = LogFactory.getLog(LocalStreamManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private AdvancedCache<K, V> cache;
   private ComponentRegistry registry;
   private StateTransferManager stm;
   private RpcManager rpc;
   private CommandsFactory factory;
   private boolean hasLoader;

   private Address localAddress;
   private CacheMode cacheMode;

   private final ConcurrentMap<Object, SegmentListener> changeListener = CollectionFactory.makeConcurrentMap();
   private ByteString cacheName;

   class SegmentListener {
      private final Set<Integer> segments;
      private final SegmentAwareOperation op;
      private final Set<Integer> segmentsLost;

      SegmentListener(Set<Integer> segments, SegmentAwareOperation op) {
         this.segments = new HashSet<>(segments);
         this.op = op;
         this.segmentsLost = new HashSet<>();
      }

      public void localSegments(Set<Integer> localSegments) {
         segments.forEach(s -> {
            if (!localSegments.contains(s)) {
               if (trace) {
                  log.tracef("Could not process segment %s", s);
               }
               segmentsLost.add(s);
            }
         });
      }

      public void lostSegments(Set<Integer> lostSegments) {
         for (Integer segment : lostSegments) {
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

   @Inject
   public void inject(Cache<K, V> cache, ComponentRegistry registry, StateTransferManager stm, RpcManager rpc,
           Configuration configuration, CommandsFactory factory) {
      // We need to unwrap the cache as a local stream should only deal with BOXED values and obviously only
      // with local entries.  Any mappings will be provided by the originator node in their intermediate operation
      // stack in the operation itself.
      this.cache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
      this.cacheName = ByteString.fromString(cache.getName());
      this.registry = registry;
      this.stm = stm;
      this.rpc = rpc;
      this.factory = factory;
      this.hasLoader = configuration.persistence().usingStores();
   }

   @Start
   public void start() {
      localAddress = rpc.getAddress();
      cache.addListener(this);
      cacheMode = cache.getCacheConfiguration().clustering().cacheMode();
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
            Set<Integer> beforeSegments = new HashSet<>(startHash.getSegmentsForOwner(localAddress));
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

   private Stream<CacheEntry<K, V>> getStream(CacheSet<CacheEntry<K, V>> cacheEntrySet, boolean parallelStream,
           Set<Integer> segments, Set<K> keysToInclude, Set<K> keysToExclude) {
      Stream<CacheEntry<K, V>> stream = (parallelStream ? cacheEntrySet.parallelStream() : cacheEntrySet.stream())
              .filterKeys(keysToInclude).filterKeySegments(segments);
      if (cacheMode.isScattered()) {
         stream = stream.filter(entry -> entry.getValue() != null);
      }
      if (!keysToExclude.isEmpty()) {
         return stream.filter(e -> !keysToExclude.contains(e.getKey()));
      }
      return stream;
   }

   private Stream<CacheEntry<K, V>> getRehashStream(CacheSet<CacheEntry<K, V>> cacheEntrySet, Object requestId,
           SegmentListener listener, boolean parallelStream, Set<Integer> segments, Set<K> keysToInclude,
           Set<K> keysToExclude) {
      CacheTopology topology = stm.getCacheTopology();
      if (trace) {
         log.tracef("Topology for supplier is %s for id %s", topology, requestId);
      }
      ConsistentHash readCH = topology.getCurrentCH();
      ConsistentHash pendingCH = topology.getPendingCH();
      if (pendingCH != null) {
         Set<Integer> lostSegments = new HashSet<>();
         Iterator<Integer> iterator = segments.iterator();
         while (iterator.hasNext()) {
            Integer segment = iterator.next();
            // This is to ensure we only unbox the value once, as below will twice most times (happy path)
            int intSegment = segment.intValue();
            // If the segment is not owned by both CHs we can't use it during rehash
            if (!pendingCH.locateOwnersForSegment(intSegment).contains(localAddress)
                    || !readCH.locateOwnersForSegment(intSegment).contains(localAddress)) {
               iterator.remove();
               lostSegments.add(segment);
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
         Set<Integer> ourSegments = readCH.getSegmentsForOwner(localAddress);
         if (segments.retainAll(ourSegments)) {
            if (trace) {
               log.tracef("We found to be missing some segments requested for id %s", requestId);
            }
            listener.localSegments(ourSegments);
         } else if (trace) {
            log.tracef("Hash %s for id %s", readCH, requestId);
         }
      }

      return getStream(cacheEntrySet, parallelStream, segments, keysToInclude, keysToExclude);
   }

   @Override
   public <R> void streamOperation(Object requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, TerminalOperation<R> operation) {
      if (trace) {
         log.tracef("Received operation request for id %s from %s for segments %s", requestId, origin, segments);
      }
      CacheSet<CacheEntry<K, V>> cacheEntrySet = getCacheRespectingLoader(includeLoader).cacheEntrySet();
      operation.setSupplier(() -> getStream(cacheEntrySet, parallelStream, segments, keysToInclude, keysToExclude));
      operation.handleInjection(registry);
      R value = operation.performOperation();
      rpc.invokeRemotely(Collections.singleton(origin), factory.buildStreamResponseCommand(requestId, true,
              Collections.emptySet(), value), rpc.getDefaultRpcOptions(true));
   }

   @Override
   public <R> void streamOperationRehashAware(Object requestId, Address origin, boolean parallelStream,
           Set<Integer> segments, Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader,
           TerminalOperation<R> operation) {
      if (trace) {
         log.tracef("Received rehash aware operation request for id %s from %s for segments %s", requestId, origin,
                 segments);
      }
      CacheSet<CacheEntry<K, V>> cacheEntrySet = getCacheRespectingLoader(includeLoader).cacheEntrySet();
      SegmentListener listener = new SegmentListener(segments, operation);
      R value;

      operation.handleInjection(registry);
      // We currently only allow 1 request per id (we may change this later)
      changeListener.put(requestId, listener);
      if (trace) {
         log.tracef("Registered change listener for %s", requestId);
      }
      try {
         operation.setSupplier(() -> getRehashStream(cacheEntrySet, requestId, listener, parallelStream, segments,
                 keysToInclude, keysToExclude));
         value = operation.performOperation();
         if (trace) {
            log.tracef("Request %s completed for segments %s with %s suspected segments", requestId, segments,
                    listener.segmentsLost);
         }
      } finally {
         changeListener.remove(requestId);
         if (trace) {
            log.tracef("UnRegistered change listener for %s", requestId);
         }
      }
      if (cache.getStatus() != ComponentStatus.RUNNING) {
         if (trace) {
            log.tracef("Cache status is no longer running, all segments are now suspect for %s", requestId);
         }
         listener.segmentsLost.addAll(segments);
         value = null;
      }

      if (trace) {
         log.tracef("Sending response for %s", requestId);
      }
      rpc.invokeRemotely(Collections.singleton(origin), factory.buildStreamResponseCommand(requestId, true,
              listener.segmentsLost, value), rpc.getDefaultRpcOptions(true));
      if (trace) {
         log.tracef("Sent response for %s", requestId);
      }
   }

   @Override
   public <R> void streamOperation(Object requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, R, ?> operation) {
      if (trace) {
         log.tracef("Received key aware operation request for id %s from %s for segments %s", requestId, origin,
                 segments);
      }
      CacheSet<CacheEntry<K, V>> cacheEntrySet = getCacheRespectingLoader(includeLoader).cacheEntrySet();
      operation.setSupplier(() -> getStream(cacheEntrySet, parallelStream, segments, keysToInclude, keysToExclude));
      operation.handleInjection(registry);
      Collection<R> value = operation.performOperation(new NonRehashIntermediateCollector<>(origin, requestId,
              parallelStream));
      rpc.invokeRemotely(Collections.singleton(origin), factory.buildStreamResponseCommand(requestId, true,
              Collections.emptySet(), value), rpc.getDefaultRpcOptions(true));
   }

   @Override
   public <R2> void streamOperationRehashAware(Object requestId, Address origin, boolean parallelStream,
           Set<Integer> segments, Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, ?, R2> operation) {
      if (trace) {
         log.tracef("Received key rehash aware operation request for id %s from %s for segments %s", requestId, origin,
                 segments);
      }
      CacheSet<CacheEntry<K, V>> cacheEntrySet = getCacheRespectingLoader(includeLoader).cacheEntrySet();
      SegmentListener listener = new SegmentListener(segments, operation);
      Collection<CacheEntry<K, R2>> results;

      operation.handleInjection(registry);
      // We currently only allow 1 request per id (we may change this later)
      changeListener.put(requestId, listener);
      if (trace) {
         log.tracef("Registered change listener for %s", requestId);
      }
      try {
         operation.setSupplier(() -> getRehashStream(cacheEntrySet, requestId, listener, parallelStream, segments,
                 keysToInclude, keysToExclude));
         results = operation.performOperationRehashAware(new NonRehashIntermediateCollector<>(origin, requestId,
                 parallelStream));
         if (trace) {
            log.tracef("Request %s completed segments %s with %s suspected segments", requestId, segments,
                    listener.segmentsLost);
         }
      } finally {
         changeListener.remove(requestId);
         if (trace) {
            log.tracef("UnRegistered change listener for %s", requestId);
         }
      }
      if (cache.getStatus() != ComponentStatus.RUNNING) {
         if (trace) {
            log.tracef("Cache status is no longer running, all segments are now suspect for %s", requestId);
         }
         listener.segmentsLost.addAll(segments);
         results = null;
      }

      rpc.invokeRemotely(Collections.singleton(origin), factory.buildStreamResponseCommand(requestId, true,
              listener.segmentsLost, results), rpc.getDefaultRpcOptions(true));
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
            rpc.invokeRemotely(Collections.singleton(origin), new StreamResponseCommand<>(cacheName, localAddress,
                    requestId, false, response), rpc.getDefaultRpcOptions(true));
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
                  rpc.invokeRemotely(Collections.singleton(origin), new StreamResponseCommand<>(cacheName, localAddress,
                          requestId, false, response), rpc.getDefaultRpcOptions(true));
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
