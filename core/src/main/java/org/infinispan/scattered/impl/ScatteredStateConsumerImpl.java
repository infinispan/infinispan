package org.infinispan.scattered.impl;

import net.jcip.annotations.GuardedBy;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.InboundTransferTask;
import org.infinispan.statetransfer.StateConsumerImpl;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.context.Flag.*;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteredStateConsumerImpl extends StateConsumerImpl {
   protected static final long SKIP_OWNERSHIP_FLAGS = EnumUtil.bitSetOf(SKIP_OWNERSHIP_CHECK);

   protected InternalEntryFactory entryFactory;
   protected ExecutorService asyncExecutor;
   protected ScatteredVersionManager svm;

   @GuardedBy("transferMapsLock")
   protected Set<Integer> inboundSegments;
   protected volatile MaxVersionsRequest maxVersions;

   protected AtomicLong chunkCounter = new AtomicLong();

   protected final ConcurrentMap<Address, BlockingQueue<Object>> retrievedEntries = new ConcurrentHashMap<>();
   protected BlockingQueue<InternalCacheEntry> backupQueue;
   protected Set<Address> backupAddress;
   private int chunkSize;

   @Inject
   public void inject(InternalEntryFactory entryFactory, ComponentRegistry componentRegistry,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService, // the same executor as in StateProviderImpl
                      ScatteredVersionManager svm) {
      this.entryFactory = entryFactory;
      this.asyncExecutor = executorService;
      this.svm = svm;
   }

   @Override
   public void start() {
      super.start();
      chunkSize = configuration.clustering().stateTransfer().chunkSize();
      backupQueue = new ArrayBlockingQueue<>(chunkSize);
      // we need to ignore nodes that don't have the cache started yet but broadcast call would reach them.
   }

   @Override
   public void onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      Address nextMember = getNextMember(cacheTopology);
      backupAddress = nextMember == null ? Collections.emptySet() : Collections.singleton(nextMember);
      super.onTopologyUpdate(cacheTopology, isRebalance);
   }

   @Override
   protected void beforeTopologyInstalled(int topologyId, boolean startRebalance, ConsistentHash previousWriteCh, ConsistentHash newWriteCh) {
      // We have to block access to segments before the topology is installed, as otherwise
      // during remote reads PrefetchInvalidationInterceptor would not retrieve remote value
      // and ScatteringInterceptor would check according to the new CH.
      Set<Integer> addedSegments = getOwnedSegments(newWriteCh);
      if (previousWriteCh != null) {
         addedSegments.removeAll(getOwnedSegments(previousWriteCh));
      }
      if (topologyId == 1) {
         log.trace("This is the first topology, not expecting any state transfer.");
         return;
      }
      if (!addedSegments.isEmpty()) {
         svm.setValuesTransferTopology(topologyId);
      }
      for (int segment : addedSegments) {
         svm.registerSegment(segment);
      }
   }

   @Override
   protected void handleSegments(boolean startRebalance, Set<Integer> addedSegments, Set<Integer> removedSegments) {
      if (!startRebalance) {
         log.trace("This is not a rebalance, not doing anything...");
         return;
      }
      for (int segment : removedSegments) {
         svm.unregisterSegment(segment);
      }
      if (addedSegments.isEmpty()) {
         log.trace("No segments missing");
         return;
      }

      synchronized (transferMapsLock) {
         inboundSegments = new HashSet<>(addedSegments);
      }
      chunkCounter.set(0);

      StateRequestCommand getMaxVersions = commandsFactory.buildStateRequestCommand(
         StateRequestCommand.Type.GET_MAX_VERSIONS, rpcManager.getAddress(),
         cacheTopology.getTopologyId(), addedSegments);

      maxVersions = new MaxVersionsRequest(cacheTopology.getActualMembers().size());
      rpcManager.invokeRemotelyAsync(cacheTopology.getActualMembers(), getMaxVersions, rpcOptions)
         .whenComplete((responseMap, throwable) -> {
            if (throwable != null) {
               maxVersions.completeExceptionally(throwable);
               return;
            }
            try {
               for (Response response : responseMap.values()) {
                  if (response.isSuccessful()) {
                     maxVersions.accept((Map<Integer, Long>) ((SuccessfulResponse) response).getResponseValue());
                  } else {
                     maxVersions.completeExceptionally(new CacheException("Unexpected response: " + response).fillInStackTrace());
                  }
               }
            } catch (Throwable t) {
               maxVersions.completeExceptionally(t);
            }
         });
      // execute locally as well
      asyncExecutor.execute(() -> {
         commandsFactory.initializeReplicableCommand(getMaxVersions, false);
         try {
            maxVersions.accept((Map<Integer, Long>) getMaxVersions.perform(null));
         } catch (Throwable throwable) {
            maxVersions.completeExceptionally(throwable);
         }
      });

      maxVersions.thenAccept(versions -> {
         log.trace("Segment versions are: " + versions);
         for (Map.Entry<Integer, Long> pair : versions.entrySet()) {
            svm.setSegmentVersion(pair.getKey(), pair.getValue());
         }
         boolean isTransferingKeys = false;

         synchronized (transferMapsLock) {
            List<Address> members = new ArrayList<>(cacheTopology.getActualMembers());
            // Reorder the member set to distibute load more evenly
            Collections.shuffle(members);
            for (Address source : members) {
               if (rpcManager.getAddress().equals(source)) {
                  continue;
               }
               isTransferingKeys = true;
               Set<Integer> segments = versions.keySet();
               InboundTransferTask inboundTransfer = new InboundTransferTask(segments, source,
                     cacheTopology.getTopologyId(), this::onTaskCompletion,
                     rpcManager, commandsFactory, configuration.clustering().stateTransfer().timeout(), cacheName);
               addTransfer(inboundTransfer, segments);
               stateRequestCompletionService.submit(() -> {
                  log.tracef("Requesting keys for segments %s from %s", inboundTransfer.getSegments(), inboundTransfer.getSource());
                  if (!inboundTransfer.requestKeys()) {
                     log.trace("Did not request any keys, setting segments to owned.");
                     svm.setOwnedSegments(segments);
                     return null;
                  }
                  if (trace)
                     log.tracef("Waiting for inbound keys transfer to finish: %s", inboundTransfer);
                  stateRequestCompletionService.continueTaskInBackground();
                  return null;
               });
            }
         }
         if (!isTransferingKeys) {
            log.trace("No keys in transfer, finishing segments " + versions.keySet());
            for (int segment : versions.keySet()) {
               svm.keyTransferFinished(segment, false);
            }
            notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());
         }
      }).exceptionally(throwable -> {
         if (cache.getAdvancedCache().getComponentRegistry().getStatus() == ComponentStatus.RUNNING) {
            log.failedRetrievingMaxVersions(throwable);
         } else {
            // reduce verbosity for stopping cache
            log.debug("Failed retrieving max versions", throwable);
         }
         for (int segment : addedSegments) {
            svm.keyTransferFinished(segment, false);
         }
         notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());
         return null;
      });
   }

   @Override
   protected void onTaskCompletion(InboundTransferTask inboundTransfer) {
      try {
         stateRequestCompletionService.backgroundTaskFinished(null);

         // a bit of overkill since we start these tasks for single segment
         Set<Integer> completedSegments = Collections.emptySet();
         if (inboundTransfer.isCompletedSuccessfully()) {
            if (trace) log.tracef("Inbound transfer finished: %s", inboundTransfer);
            synchronized (transferMapsLock) {
               // transferMapsLock is held when all the tasks are added so we see that all of them are done
               for (int segment : inboundTransfer.getSegments()) {
                  List<InboundTransferTask> transfers = transfersBySegment.get(segment);
                  if (transfers == null) {
                     // It is possible that two task complete concurrently, one of them checks is all tasks
                     // for given segments have been completed successfully and (finding out that it's true)
                     // removes the transfer for given segment. The second task arrives and finds out that
                     // its record int transfersBySegment is gone, but that's OK, as the segment has been handled.
                     log.tracef("Transfers for segment %d have not been found.", segment);
                  } else {
                     // We are removing here rather than in removeTransfer, because we need to know if we're the last
                     // finishing task.
                     transfers.remove(inboundTransfer);
                     if (transfers.isEmpty()) {
                        transfersBySegment.remove(segment);
                        log.trace("All transfer tasks have completed.");
                        svm.keyTransferFinished(segment, true);
                        switch (completedSegments.size()) {
                           case 0:
                              completedSegments = Collections.singleton(segment);
                              break;
                           case 1:
                              completedSegments = new HashSet<>(completedSegments);
                              // intentional no break
                           default:
                              completedSegments.add(segment);
                        }
                     }
                  }
               }
            }
         } else if (inboundTransfer.isCancelled()) {
            removeTransfer(inboundTransfer);
            // no more work
            return;
         } else {
            // TODO retry? This happens only when another node dies, so we may have lost the data
            throw new UnsupportedOperationException();
         }

         if (completedSegments.isEmpty()) {
            log.tracef("Not requesting any values yet because no segments have been completed.");
         } else {
            Set<Integer> finalCompletedSegments = completedSegments;
            log.tracef("Requesting values from segments %s, for in-memory keys", finalCompletedSegments);
            // for all keys from given segment request values
            ConsistentHash readCh = cacheTopology.getReadConsistentHash();
            for (InternalCacheEntry ice : dataContainer) {
               Object key = ice.getKey();
               int segmentId = readCh.getSegment(key);
               if (finalCompletedSegments.contains(segmentId)) {
                  if (ice.getMetadata() instanceof RemoteMetadata) {
                     retrieveEntry(ice.getKey(), ((RemoteMetadata) ice.getMetadata()).getAddress());
                  } else {
                     backupEntry(ice);
                  }
               }
            }

            // With passivation, some key could be activated here and we could miss it,
            // but then it should be broadcast-loaded in PrefetchInvalidationInterceptor
            AdvancedCacheLoader stProvider = persistenceManager.getStateTransferProvider();
            if (stProvider != null) {
               try {
                  CollectionKeyFilter filter = new CollectionKeyFilter(new ReadOnlyDataContainerBackedKeySet(dataContainer));
                  AdvancedCacheLoader.CacheLoaderTask task = (me, taskContext) -> {
                     int segmentId = readCh.getSegment(me.getKey());
                     if (finalCompletedSegments.contains(segmentId)) {
                        try {
                           InternalMetadata metadata = me.getMetadata();
                           if (metadata instanceof RemoteMetadata) {
                              retrieveEntry(me.getKey(), ((RemoteMetadata) metadata).getAddress());
                           } else {
                              backupEntry(entryFactory.create(me.getKey(), me.getValue(), me.getMetadata()));
                           }
                        } catch (CacheException e) {
                           log.failedLoadingValueFromCacheStore(me.getKey(), e);
                        }
                     }
                  };
                  stProvider.process(filter, task, new WithinThreadExecutor(), true, true);
               } catch (CacheException e) {
                  log.failedLoadingKeysFromCacheStore(e);
               }
            }
         }

         boolean lastTransfer = false;
         synchronized (transferMapsLock) {
            inboundSegments.removeAll(completedSegments);
            log.tracef("Unfinished inbound segments: " + inboundSegments);
            if (inboundSegments.isEmpty()) {
               lastTransfer = true;
            }
         }

         if (lastTransfer) {
            for (Map.Entry<Address, BlockingQueue<Object>> pair : retrievedEntries.entrySet()) {
               BlockingQueue<Object> queue = pair.getValue();
               List<Object> keys = new ArrayList<>(queue.size());
               queue.drainTo(keys);
               if (!keys.isEmpty()) {
                  getValuesAndApply(pair.getKey(), keys);
               }
            }
            List<InternalCacheEntry> entries = new ArrayList<>(backupQueue.size());
            backupQueue.drainTo(entries);
            if (!entries.isEmpty()) {
               backupEntries(entries);
            }
         }

         // we must not remove the transfer before the requests for values are sent
         // as we could notify the end of rebalance too soon
         removeTransfer(inboundTransfer);
         if (chunkCounter.get() == 0) {
            notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());
         }
      } catch (Throwable t) {
         log.error("Damned", t);
         throw t;
      }
   }

   private void backupEntry(InternalCacheEntry entry) {
      // we had the last version of the entry and are becoming a primary owner, so we have to back it up
      List<InternalCacheEntry> entries = null;
      if (backupQueue.offer(entry)) {
         if (backupQueue.size() >= chunkSize) {
            entries = new ArrayList<>(chunkSize);
            backupQueue.drainTo(entries, chunkSize);
         }
      } else {
         entries = new ArrayList<>(chunkSize);
         entries.add(entry);
         backupQueue.drainTo(entries, chunkSize - 1);
      }
      if (entries != null && !entries.isEmpty()) {
         backupEntries(entries);
      }
   }

   private void backupEntries(List<InternalCacheEntry> entries) {
      chunkCounter.incrementAndGet();
      Map<Object, InternalCacheValue> map = new HashMap<>();
      for (InternalCacheEntry entry : entries) {
         map.put(entry.getKey(), entry.toInternalCacheValue());
      }
      PutMapCommand putMapCommand = commandsFactory.buildPutMapCommand(map, null, STATE_TRANSFER_FLAGS);
      rpcManager.invokeRemotelyAsync(backupAddress, putMapCommand, rpcOptions).whenComplete(((responseMap, throwable) -> {
         try {
            if (throwable != null) {
               log.failedOutBoundTransferExecution(throwable);
            }
         } finally {
            if (chunkCounter.decrementAndGet() == 0) {
               notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());
            }
         }
      }));
   }

   private void retrieveEntry(Object key, Address address) {
      BlockingQueue<Object> queue = retrievedEntries.computeIfAbsent(address, k -> new ArrayBlockingQueue<>(chunkSize));
      // in concurrent case there could be multiple retrievals
      List<Object> keys = null;
      if (queue.offer(key)) {
         if (queue.size() >= chunkSize) {
            keys = new ArrayList<>(chunkSize);
            queue.drainTo(keys, chunkSize);
         }
      } else {
         keys = new ArrayList<>(chunkSize);
         keys.add(key);
         queue.drainTo(keys, chunkSize - 1);
      }
      if (keys != null && !keys.isEmpty()) {
         getValuesAndApply(address, keys);
      }
   }

   private void getValuesAndApply(Address address, List<Object> keys) {
      // TODO: throttle the number of commands sent, otherwise we could DDoS self
      chunkCounter.incrementAndGet();
      ClusteredGetAllCommand command = commandsFactory.buildClusteredGetAllCommand(keys, SKIP_OWNERSHIP_FLAGS, null);
      rpcManager.invokeRemotelyAsync(Collections.singleton(address), command, rpcManager.getDefaultRpcOptions(true))
         .whenComplete((responseMap, throwable) -> {
            // TODO: this is executed in OOB-thread, should we move it to stateTransferExecutor?
            try {
               if (throwable != null) {
                  log.exceptionProcessingEntryRetrievalValues(throwable);
               } else {
                  Response response = responseMap.get(address);
                  if (response == null) {
                     throw new CacheException("Did not get response from " + address + ", got " + responseMap);
                  } else if (!response.isSuccessful()) {
                     throw new CacheException("Response from " + address + " is unsuccessful: " + response);
                  }
                  List<InternalCacheValue> values = (List<InternalCacheValue>) ((SuccessfulResponse) response).getResponseValue();
                  for (int i = 0; i < keys.size(); ++i) {
                     Object key = keys.get(i);
                     InternalCacheValue icv = values.get(i);
                     PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(key, icv.getValue(), icv.getMetadata(), STATE_TRANSFER_FLAGS);
                     try {
                        interceptorChain.invoke(icf.createSingleKeyNonTxInvocationContext(), put);
                     } catch (Exception e) {
                        if (!cache.getStatus().allowInvocations()) {
                           log.debugf("Cache %s is shutting down, stopping state transfer", cacheName);
                           break;
                        } else {
                           log.problemApplyingStateForKey(e.getMessage(), key, e);
                        }
                     }
                  }
               }
            } catch (Exception e) {
               log.error("Failed to process values during state transfer", e);
               throw e;
            } finally {
               if (chunkCounter.decrementAndGet() == 0) {
                  notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());
               }
            }
         });
   }

   @Override
   public void stopApplyingState(int topologyId) {
      svm.valuesReceived(topologyId);
      super.stopApplyingState(topologyId);
   }

   @Override
   protected boolean notifyEndOfRebalanceIfNeeded(int topologyId, int rebalanceId) {
      if (super.notifyEndOfRebalanceIfNeeded(topologyId, rebalanceId)) {
         ConsistentHash consistentHash = cacheTopology.getWriteConsistentHash();
         Address localAddress = rpcManager.getAddress();
         if (consistentHash.getMembers().contains(localAddress)) {
            svm.setOwnedSegments(consistentHash.getSegmentsForOwner(localAddress));
         }
         return true;
      }
      return false;
   }

   @Override
   protected void removeStaleData(Set<Integer> removedSegments) throws InterruptedException {
      svm.setNonOwnedSegments(removedSegments);
   }

   @Override
   public boolean hasActiveTransfers() {
      MaxVersionsRequest maxVersions = this.maxVersions;
      return (maxVersions != null && !maxVersions.isDone()) || super.hasActiveTransfers();
   }

   private Address getNextMember(CacheTopology cacheTopology) {
      Address myAddress = rpcManager.getAddress();
      List<Address> members = cacheTopology.getActualMembers();
      if (members.size() <= 1) {
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
      // I am not a member of the topology (joining)
      return null;
   }

   private static class MaxVersionsRequest extends CompletableFuture<Map<Integer, Long>> {
      private int counter;
      private Map<Integer, Long> versions = new HashMap<>();

      public MaxVersionsRequest(int numResponses) {
         counter = numResponses;
      }

      public synchronized void accept(Map<Integer, Long> versions) {
         if (trace) {
            log.trace("Received max versions " + versions);
         }
         for (Map.Entry<Integer, Long> pair : versions.entrySet()) {
            this.versions.compute(pair.getKey(), (s, existing) -> existing == null ? pair.getValue() : Math.max(pair.getValue(), existing));
         }
         if (--counter == 0) {
            complete(this.versions);
         }
      }
   }
}
