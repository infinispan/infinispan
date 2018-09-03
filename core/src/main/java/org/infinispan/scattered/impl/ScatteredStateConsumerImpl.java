package org.infinispan.scattered.impl;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.InboundTransferTask;
import org.infinispan.statetransfer.StateConsumerImpl;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.Flowable;
import net.jcip.annotations.GuardedBy;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteredStateConsumerImpl extends StateConsumerImpl {
   private static final Log log = LogFactory.getLog(ScatteredStateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   protected static final long SKIP_OWNERSHIP_FLAGS = FlagBitSets.SKIP_OWNERSHIP_CHECK;

   @Inject protected InternalEntryFactory entryFactory;
   @Inject @ComponentName(ASYNC_TRANSPORT_EXECUTOR)
   protected ExecutorService asyncExecutor;
   @Inject protected ScatteredVersionManager svm;

   @GuardedBy("transferMapsLock")
   protected IntSet inboundSegments;

   protected AtomicLong chunkCounter = new AtomicLong();

   protected final ConcurrentMap<Address, BlockingQueue<Object>> retrievedEntries = new ConcurrentHashMap<>();
   protected BlockingQueue<InternalCacheEntry> backupQueue;
   protected final ConcurrentMap<Address, BlockingQueue<KeyAndVersion>> invalidations = new ConcurrentHashMap<>();
   protected Collection<Address> backupAddress;
   protected Collection<Address> nonBackupAddresses;
   private int chunkSize;

   @Override
   public void start() {
      super.start();
      chunkSize = configuration.clustering().stateTransfer().chunkSize();
      backupQueue = new ArrayBlockingQueue<>(chunkSize);
      // we need to ignore nodes that don't have the cache started yet but broadcast call would reach them.
   }

   @Override
   public CompletableFuture<Void> onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      Address nextMember = getNextMember(cacheTopology);
      backupAddress = nextMember == null ? Collections.emptySet() : Collections.singleton(nextMember);
      nonBackupAddresses = new ArrayList<>(cacheTopology.getActualMembers());
      nonBackupAddresses.remove(nextMember);
      nonBackupAddresses.remove(rpcManager.getAddress());
      return super.onTopologyUpdate(cacheTopology, isRebalance);
   }

   @Override
   protected void beforeTopologyInstalled(int topologyId, boolean startRebalance, ConsistentHash previousWriteCh, ConsistentHash newWriteCh) {
      // We have to block access to segments before the topology is installed, as otherwise
      // during remote reads PrefetchInvalidationInterceptor would not retrieve remote value
      // and ScatteringInterceptor would check according to the new CH.
      for (int segment = 0; segment < newWriteCh.getNumSegments(); ++segment) {
         if (!newWriteCh.isSegmentLocalToNode(rpcManager.getAddress(), segment)) {
            // Failed key transfer could move segment to OWNED state concurrently with installing a new topology.
            // Therefore we cancel the transfers before installing the topology, preventing any unexpected segment
            // state updates. Cancelling moves the segments to NOT_OWNED state.
            cancelTransfers(IntSets.immutableSet(segment));
            svm.unregisterSegment(segment);
         }
      }
      IntSet addedSegments = getOwnedSegments(newWriteCh);
      // We check if addedSegments is empty, because we may receive an immutable empty set back from getOwnedSegments
      if (previousWriteCh != null && !addedSegments.isEmpty()) {
         addedSegments.removeAll(getOwnedSegments(previousWriteCh));
      }
      svm.setTopologyId(topologyId);
      if (previousWriteCh == null || !isFetchEnabled) {
         log.trace("This is the first topology or state transfer is disabled, not expecting any state transfer.");
         svm.setOwnedSegments(addedSegments);
         return;
      }
      if (!addedSegments.isEmpty()) {
         svm.setValuesTransferTopology(topologyId);
         for (PrimitiveIterator.OfInt segmentIterator = addedSegments.iterator(); segmentIterator.hasNext(); ) {
            svm.registerSegment(segmentIterator.nextInt());
         }
      }
   }

   @Override
   protected void handleSegments(boolean startRebalance, IntSet addedSegments, IntSet removedSegments) {
      if (!startRebalance) {
         log.trace("This is not a rebalance, not doing anything...");
         return;
      }
      if (addedSegments.isEmpty()) {
         log.trace("No segments missing");
         return;
      }

      synchronized (transferMapsLock) {
         inboundSegments = IntSets.mutableFrom(addedSegments);
      }
      chunkCounter.set(0);
      if (trace)
         log.tracef("Revoking all segments, chunk counter reset to 0");

      StateRequestCommand command = commandsFactory.buildStateRequestCommand(
            StateRequestCommand.Type.CONFIRM_REVOKED_SEGMENTS,
            rpcManager.getAddress(), cacheTopology.getTopologyId(), addedSegments);
            // we need to wait synchronously for the completion
      rpcManager.blocking(
            rpcManager.invokeCommandOnAll(command, MapResponseCollector.ignoreLeavers(),
                                          rpcManager.getSyncRpcOptions()).whenComplete((responses, throwable) -> {
         if (throwable == null) {
            try {
               svm.startKeyTransfer(addedSegments);
               requestKeyTransfer(addedSegments);
            } catch (Throwable t) {
               log.failedToRequestSegments(cacheName, null, addedSegments, t);
            }
         } else {
            if (cache.wired().getStatus() == ComponentStatus.RUNNING) {
               log.failedConfirmingRevokedSegments(throwable);
            } else {
               // reduce verbosity for stopping cache
               log.debug("Failed confirming revoked segments", throwable);
            }
            for (int segment : addedSegments) {
               svm.notifyKeyTransferFinished(segment, false, false);
            }
            notifyEndOfStateTransferIfNeeded();
         }
      }));
   }

   private void requestKeyTransfer(IntSet segments) {
      boolean isTransferringKeys = false;

      synchronized (transferMapsLock) {
         List<Address> members = new ArrayList<>(cacheTopology.getActualMembers());
         // Reorder the member set to distibute load more evenly
         Collections.shuffle(members);
         for (Address source : members) {
            if (source.equals(rpcManager.getAddress())) {
               continue;
            }
            isTransferringKeys = true;
            InboundTransferTask inboundTransfer = new InboundTransferTask(segments, source,
                  cacheTopology.getTopologyId(), rpcManager, commandsFactory,
                  configuration.clustering().stateTransfer().timeout(), cacheName, true);
            addTransfer(inboundTransfer, segments);
            stateRequestExecutor.executeAsync(() -> {
               log.tracef("Requesting keys for segments %s from %s", inboundTransfer.getSegments(), inboundTransfer.getSource());
               return inboundTransfer.requestKeys().whenComplete((nil, e) -> onTaskCompletion(inboundTransfer));
            });
         }
      }
      if (!isTransferringKeys) {
         log.trace("No keys in transfer, finishing segments " + segments);
         for (int segment : segments) {
            svm.notifyKeyTransferFinished(segment, false, false);
         }
         notifyEndOfStateTransferIfNeeded();
      }
   }

   @Override
   protected void onTaskCompletion(InboundTransferTask inboundTransfer) {
      // a bit of overkill since we start these tasks for single segment
      IntSet completedSegments = IntSets.immutableEmptySet();
      if (trace) log.tracef("Inbound transfer finished %s: %s", inboundTransfer,
            inboundTransfer.isCompletedSuccessfully() ? "successfully" : "unsuccessfuly");
      synchronized (transferMapsLock) {
         // transferMapsLock is held when all the tasks are added so we see that all of them are done
         for (PrimitiveIterator.OfInt iter = inboundTransfer.getSegments().iterator(); iter.hasNext(); ) {
            int segment = iter.nextInt();
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
                  if (trace) {
                     log.tracef("All transfer tasks for segment %d have completed.", segment);
                  }
                  svm.notifyKeyTransferFinished(segment, inboundTransfer.isCompletedSuccessfully(), inboundTransfer.isCancelled());
                  switch (completedSegments.size()) {
                     case 0:
                        completedSegments = IntSets.immutableSet(segment);
                        break;
                     case 1:
                        completedSegments = IntSets.mutableCopyFrom(completedSegments);
                        // Intentional falls through
                     default:
                        completedSegments.add(segment);
                  }
               }
            }
         }
      }

      if (completedSegments.isEmpty()) {
         log.tracef("Not requesting any values yet because no segments have been completed.");
      } else if (inboundTransfer.isCompletedSuccessfully()) {
         IntSet finalCompletedSegments = completedSegments;
         log.tracef("Requesting values from segments %s, for in-memory keys", finalCompletedSegments);
         dataContainer.forEach(finalCompletedSegments, ice -> {
            // TODO: could the version be null in here?
            if (ice.getMetadata() instanceof RemoteMetadata) {
               Address backup = ((RemoteMetadata) ice.getMetadata()).getAddress();
               retrieveEntry(ice.getKey(), backup);
               for (Address member : cacheTopology.getActualMembers()) {
                  if (!member.equals(backup)) {
                     invalidate(ice.getKey(), ice.getMetadata().version(), member);
                  }
               }
            } else {
               backupEntry(ice);
               for (Address member : nonBackupAddresses) {
                  invalidate(ice.getKey(), ice.getMetadata().version(), member);
               }
            }
         });

         // With passivation, some key could be activated here and we could miss it,
         // but then it should be broadcast-loaded in PrefetchInvalidationInterceptor
         AdvancedCacheLoader<Object, Object> stProvider = persistenceManager.getStateTransferProvider();
         if (stProvider != null) {
            try {
               CollectionKeyFilter filter = new CollectionKeyFilter(new ReadOnlyDataContainerBackedKeySet(dataContainer));
               AdvancedCacheLoader.CacheLoaderTask task = (me, taskContext) -> {
                  int segmentId = keyPartitioner.getSegment(me.getKey());
                  if (finalCompletedSegments.contains(segmentId)) {
                     try {
                        InternalMetadata metadata = me.getMetadata();
                        if (metadata instanceof RemoteMetadata) {
                           Address backup = ((RemoteMetadata) metadata).getAddress();
                           retrieveEntry(me.getKey(), backup);
                           for (Address member : cacheTopology.getActualMembers()) {
                              if (!member.equals(backup)) {
                                 invalidate(me.getKey(), metadata.version(), member);
                              }
                           }
                        } else {
                           backupEntry(entryFactory.create(me.getKey(), me.getValue(), me.getMetadata()));
                           for (Address member : nonBackupAddresses) {
                              invalidate(me.getKey(), metadata.version(), member);
                           }
                        }
                     } catch (CacheException e) {
                        log.failedLoadingValueFromCacheStore(me.getKey(), e);
                     }
                  }
               };
               Flowable.fromPublisher(stProvider.publishEntries(filter::accept, true, true))
                     .blockingForEach(me -> task.processEntry(me, null));
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
         for (Map.Entry<Address, BlockingQueue<KeyAndVersion>> pair : invalidations.entrySet()) {
            BlockingQueue<KeyAndVersion> queue = pair.getValue();
            List<KeyAndVersion> list = new ArrayList<>(queue.size());
            queue.drainTo(list);
            if (!list.isEmpty()) {
               invalidate(list, pair.getKey());
            }
         }
      }

      // we must not remove the transfer before the requests for values are sent
      // as we could notify the end of rebalance too soon
      removeTransfer(inboundTransfer);
      if (trace)
         log.tracef("Inbound transfer removed, chunk counter is %s", chunkCounter.get());
      if (chunkCounter.get() == 0) {
         notifyEndOfStateTransferIfNeeded();
      }
   }

   private <T> List<T> offerAndDrain(BlockingQueue<T> queue, T element) {
      List<T> list = null;
      if (queue.offer(element)) {
         if (queue.size() >= chunkSize) {
            list = new ArrayList<>(chunkSize);
            queue.drainTo(list, chunkSize);
         }
      } else {
         list = new ArrayList<>(chunkSize);
         list.add(element);
         queue.drainTo(list, chunkSize - 1);
      }
      return list;
   }

   private void invalidate(Object key, EntryVersion version, Address member) {
      BlockingQueue<KeyAndVersion> queue = invalidations.computeIfAbsent(member, m -> new ArrayBlockingQueue<>(chunkSize));
      List<KeyAndVersion> list = offerAndDrain(queue, new KeyAndVersion(key, version));
      if (list != null && !list.isEmpty()) {
         invalidate(list, member);
      }
   }

   private void invalidate(List<KeyAndVersion> list, Address member) {
      Object[] keys = new Object[list.size()];
      int[] topologyIds = new int[list.size()];
      long[] versions = new long[list.size()];
      int i = 0;
      for (KeyAndVersion pair : list) {
         keys[i] = pair.key;
         SimpleClusteredVersion version = (SimpleClusteredVersion) pair.version;
         topologyIds[i] = version.topologyId;
         versions[i] = version.version;
         ++i;
      }
      // Theoretically we can just send these invalidations asynchronously, but we'd prefer to have old copies
      // removed when state transfer completes.
      long incrementedCounter = chunkCounter.incrementAndGet();
      if (trace)
         log.tracef("Invalidating versions on %s, chunk counter incremented to %d", member, incrementedCounter);
      InvalidateVersionsCommand ivc = commandsFactory.buildInvalidateVersionsCommand(cacheTopology.getTopologyId(), keys, topologyIds, versions, true);
      rpcManager.invokeCommand(member, ivc, SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions())
                .whenComplete((response, t) -> {
                   if (t != null) {
                      log.failedInvalidatingRemoteCache(t);
                   }
                   long decrementedCounter = chunkCounter.decrementAndGet();
                   if (trace)
                      log.tracef("Versions invalidated on %s, chunk counter decremented to %d", member, decrementedCounter);
                   if (decrementedCounter == 0) {
                      notifyEndOfStateTransferIfNeeded();
                   }
                });
   }

   private void backupEntry(InternalCacheEntry entry) {
      // we had the last version of the entry and are becoming a primary owner, so we have to back it up
      List<InternalCacheEntry> entries = offerAndDrain(backupQueue, entry);
      if (entries != null && !entries.isEmpty()) {
         backupEntries(entries);
      }
   }

   private void backupEntries(List<InternalCacheEntry> entries) {
      long incrementedCounter = chunkCounter.incrementAndGet();
      if (trace)
         log.tracef("Backing up entries, chunk counter is %d", incrementedCounter);
      Map<Object, InternalCacheValue> map = new HashMap<>();
      for (InternalCacheEntry entry : entries) {
         map.put(entry.getKey(), entry.toInternalCacheValue());
      }
      PutMapCommand putMapCommand = commandsFactory.buildPutMapCommand(map, null, STATE_TRANSFER_FLAGS);
      putMapCommand.setTopologyId(rpcManager.getTopologyId());
      rpcManager.invokeCommand(backupAddress, putMapCommand, SingleResponseCollector.validOnly(),
                               rpcManager.getSyncRpcOptions())
                .whenComplete(((response, throwable) -> {
         try {
            if (throwable != null) {
               log.failedOutBoundTransferExecution(throwable);
            }
         } finally {
            long decrementedCounter = chunkCounter.decrementAndGet();
            if (trace)
               log.tracef("Backed up entries, chunk counter is %d", decrementedCounter);
            if (decrementedCounter == 0) {
               notifyEndOfStateTransferIfNeeded();
            }
         }
      }));
   }

   private void retrieveEntry(Object key, Address address) {
      BlockingQueue<Object> queue = retrievedEntries.computeIfAbsent(address, k -> new ArrayBlockingQueue<>(chunkSize));
      // in concurrent case there could be multiple retrievals
      List<Object> keys = offerAndDrain(queue, key);
      if (keys != null && !keys.isEmpty()) {
         getValuesAndApply(address, keys);
      }
   }

   private void getValuesAndApply(Address address, List<Object> keys) {
      // TODO: throttle the number of commands sent, otherwise we could DDoS self
      long incrementedCounter = chunkCounter.incrementAndGet();
      if (trace)
         log.tracef("Retrieving values, chunk counter is %d", incrementedCounter);
      ClusteredGetAllCommand command = commandsFactory.buildClusteredGetAllCommand(keys, SKIP_OWNERSHIP_FLAGS, null);
      command.setTopologyId(rpcManager.getTopologyId());
      rpcManager.invokeCommand(address, command, SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions())
         .whenComplete((response, throwable) -> {
            try {
               if (throwable != null) {
                  throw log.exceptionProcessingEntryRetrievalValues(throwable);
               } else {
                  applyValues(address, keys, response);
               }
            } catch (Throwable t) {
               log.failedProcessingValuesDuringRebalance(t);
               throw t;
            } finally {
               long decrementedCounter = chunkCounter.decrementAndGet();
               if (trace)
                  log.tracef("Applied values, chunk counter is %d", decrementedCounter);
               if (decrementedCounter == 0) {
                  notifyEndOfStateTransferIfNeeded();
               }
            }
         });
   }

   private void applyValues(Address address, List<Object> keys, Response response) {
      if (response == null) {
         throw new CacheException("Did not get response from " + address);
      } else if (!response.isSuccessful()) {
         throw new CacheException("Response from " + address + " is unsuccessful: " + response);
      }
      InternalCacheValue[] values = (InternalCacheValue[]) ((SuccessfulResponse) response).getResponseValue();
      if (values == null) {
         // TODO: The other node got higher topology
         throw new IllegalStateException();
      }
      for (int i = 0; i < keys.size(); ++i) {
         Object key = keys.get(i);
         InternalCacheValue icv = values[i];
         if (icv == null) {
            // The entry got lost in the meantime - this can happen when the container is cleared concurrently to processing
            // the GetAllCommand. We'll just avoid NPEs here: data is lost as > 1 nodes have left.
            continue;
         }
         PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(key, icv.getValue(),
               keyPartitioner.getSegment(key), icv.getMetadata(), STATE_TRANSFER_FLAGS);
         try {
            interceptorChain.invoke(icf.createSingleKeyNonTxInvocationContext(), put);
         } catch (Exception e) {
            if (!cache.wired().getStatus().allowInvocations()) {
               log.debugf("Cache %s is shutting down, stopping state transfer", cacheName);
               break;
            } else {
               log.problemApplyingStateForKey(e.getMessage(), key, e);
            }
         }
      }
   }

   @Override
   public void stopApplyingState(int topologyId) {
      svm.notifyValueTransferFinished();
      super.stopApplyingState(topologyId);
   }

   @Override
   protected void removeStaleData(IntSet removedSegments) throws InterruptedException {
      // Noop - scattered cache cannot remove data even if it is not an owner
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

   protected static class KeyAndVersion {
      public final Object key;
      public final EntryVersion version;

      public KeyAndVersion(Object key, EntryVersion version) {
         this.key = key;
         this.version = version;
      }
   }
}
