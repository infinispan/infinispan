package org.infinispan.scattered.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.scattered.ScatteredStateProvider;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.OutboundTransferTask;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateProviderImpl;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteredStateProviderImpl extends StateProviderImpl implements ScatteredStateProvider {
   private static final Log log = LogFactory.getLog(ScatteredStateProviderImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject protected ScatteredVersionManager svm;

   @Override
   public void start() {
      super.start();
   }

   @Override
   public CompletableFuture<Void> onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      if (isRebalance) {
         // TODO [rvansa]: do this only when member was lost
         return replicateAndInvalidate(cacheTopology);
      } else {
         return CompletableFutures.completedNull();
      }
   }

   // This method handles creating the backup copy and invalidation on other nodes for segments
   // that this node keeps from previous topology.
   private CompletableFuture<Void> replicateAndInvalidate(CacheTopology cacheTopology) {
      Address nextMember = getNextMember(cacheTopology);
      if (nextMember != null) {
         HashSet<Address> otherMembers = new HashSet<>(cacheTopology.getActualMembers());
         Address localAddress = rpcManager.getAddress();
         otherMembers.remove(localAddress);
         otherMembers.remove(nextMember);

         Set<Integer> oldSegments = cacheTopology.getCurrentCH().getMembers().contains(localAddress) ?
               new HashSet<>(cacheTopology.getCurrentCH().getSegmentsForOwner(localAddress)) : Collections.emptySet();
         oldSegments.retainAll(cacheTopology.getPendingCH().getSegmentsForOwner(localAddress));
         log.trace("Segments to replicate and invalidate: " + oldSegments);
         if (oldSegments.isEmpty()) {
            return CompletableFutures.completedNull();
         }

         // we'll start at 1, so the counter will never drop to 0 until we send all chunks
         AtomicInteger outboundInvalidations = new AtomicInteger(1);
         CompletableFuture<Void> outboundTaskFuture = new CompletableFuture<>();
         OutboundTransferTask outboundTransferTask = new OutboundTransferTask(nextMember, oldSegments,
            chunkSize,
            cacheTopology.getTopologyId(), keyPartitioner,
            task -> {
               if (outboundInvalidations.decrementAndGet() == 0) {
                  outboundTaskFuture.complete(null);
               }
            }, chunks -> invalidateChunks(chunks, otherMembers, outboundInvalidations, outboundTaskFuture, cacheTopology),
            OutboundTransferTask::defaultMapEntryFromDataContainer, OutboundTransferTask::defaultMapEntryFromStore,
            dataContainer, persistenceManager, rpcManager, commandsFactory, entryFactory, timeout, cacheName, true, true);
         outboundTransferTask.execute(executorService);
         return outboundTaskFuture;
      } else {
         return CompletableFutures.completedNull();
      }
   }

   private void invalidateChunks(List<StateChunk> stateChunks, Set<Address> otherMembers, AtomicInteger outboundInvalidations, CompletableFuture<Void> outboundTaskFuture, CacheTopology cacheTopology) {
      int numEntries = stateChunks.stream().mapToInt(chunk -> chunk.getCacheEntries().size()).sum();
      if (numEntries == 0) {
         log.tracef("Nothing to invalidate");
         return;
      }
      Object keys[] = new Object[numEntries];
      int topologyIds[] = new int[numEntries];
      long versions[] = new long[numEntries];
      int i = 0;
      for (StateChunk chunk : stateChunks) {
         for (InternalCacheEntry entry : chunk.getCacheEntries()) {
            // we have replicated the non-versioned entries but we won't invalidate them elsewhere
            if (entry.getMetadata() != null && entry.getMetadata().version() != null) {
               keys[i] = entry.getKey();
               SimpleClusteredVersion version = (SimpleClusteredVersion) entry.getMetadata().version();
               topologyIds[i] = version.topologyId;
               versions[i] = version.version;
               ++i;
            }
         }
      }
      if (trace) {
         log.tracef("Invalidating %d entries from segments %s", numEntries, stateChunks.stream().map(chunk -> chunk.getSegmentId()).collect(Collectors.toList()));
      }
      outboundInvalidations.incrementAndGet();
      rpcManager.invokeCommand(otherMembers,
                               commandsFactory.buildInvalidateVersionsCommand(cacheTopology.getTopologyId(), keys,
                                                                              topologyIds, versions, true),
                               MapResponseCollector.ignoreLeavers(otherMembers.size()),
                               rpcManager.getSyncRpcOptions())
                .whenComplete((r, t) -> {
         try {
            if (t != null) {
               log.failedInvalidatingRemoteCache(t);
            }
         } finally {
            if (outboundInvalidations.decrementAndGet() == 0) {
               outboundTaskFuture.complete(null);
            }
         }
      });
   }

   private Address getNextMember(CacheTopology cacheTopology) {
      Address myAddress = rpcManager.getAddress();
      List<Address> members = cacheTopology.getActualMembers();
      if (members.size() == 1) {
         return null;
      }
      Iterator<Address> it = members.iterator();
      while (it.hasNext()) {
         Address member = it.next();
         if (member.equals(myAddress)) {
            if (it.hasNext()) {
               return it.next();
            } else {
               return members.get(0);
            }
         }
      }
      throw new IllegalStateException();
   }

   @Override
   public void startKeysTransfer(Set<Integer> segments, Address origin) {
      CacheTopology cacheTopology = stateConsumer.getCacheTopology();
      Address localAddress = rpcManager.getAddress();
      OutboundTransferTask outboundTransferTask = new OutboundTransferTask(origin, segments, chunkSize,
         cacheTopology.getTopologyId(), keyPartitioner,
         this::onTaskCompletion, list -> {},
         (ice, ef) -> {
            Metadata metadata = ice.getMetadata();
            if (metadata != null && metadata.version() != null) {
               return ef.create(ice.getKey(), null, new RemoteMetadata(localAddress, metadata.version()));
            } else {
               return null;
            }
         },
         (me, ef) -> {
            InternalMetadata metadata = me.getMetadata();
            if (metadata != null && metadata.version() != null) {
               return ef.create(me.getKey(), null, new RemoteMetadata(localAddress, metadata.version()));
            } else {
               return null;
            }
         }, dataContainer, persistenceManager, rpcManager, commandsFactory, entryFactory, timeout, cacheName, true, false);
      addTransfer(outboundTransferTask);
      outboundTransferTask.execute(executorService);
   }

   @Override
   public CompletableFuture<Void> confirmRevokedSegments(int topologyId) {
      return stateTransferLock.topologyFuture(topologyId);
   }
}
