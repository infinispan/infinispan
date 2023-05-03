package org.infinispan.statetransfer;

import static org.infinispan.context.Flag.STATE_TRANSFER_PROGRESS;
import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.reactive.publisher.impl.SegmentAwarePublisherSupplier;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Flowable;

/**
 * {@link StateProvider} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public class StateProviderImpl implements StateProvider {

   private static final Log log = LogFactory.getLog(StateProviderImpl.class);

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject protected String cacheName;
   @Inject Configuration configuration;
   @Inject protected RpcManager rpcManager;
   @Inject protected CommandsFactory commandsFactory;
   @Inject ClusterCacheNotifier clusterCacheNotifier;
   @Inject TransactionTable transactionTable;     // optional
   @Inject protected InternalDataContainer<Object, Object> dataContainer;
   @Inject protected PersistenceManager persistenceManager; // optional
   @Inject protected StateTransferLock stateTransferLock;
   @Inject protected InternalEntryFactory entryFactory;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected DistributionManager distributionManager;
   @Inject protected TransactionOriginatorChecker transactionOriginatorChecker;
   @Inject protected LocalPublisherManager<?, ?> localPublisherManager;
   @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   @Inject ScheduledExecutorService timeoutExecutor;

   protected long timeout;
   protected int chunkSize;

   /**
    * A map that keeps track of current outbound state transfers by destination address. There could be multiple transfers
    * flowing to the same destination (but for different segments) so the values are lists.
    */
   private final Map<Address, List<OutboundTransferTask>> transfersByDestination = new HashMap<>();

   /**
    * Flags used when requesting the local publisher for the entries.
    */
   private static final long STATE_TRANSFER_ENTRIES_FLAGS = EnumUtil.bitSetOf(
         // Indicate the command to not use shared stores.
         STATE_TRANSFER_PROGRESS
   );

   public StateProviderImpl() {
   }

   public boolean isStateTransferInProgress() {
      synchronized (transfersByDestination) {
         return !transfersByDestination.isEmpty();
      }
   }

   public CompletableFuture<Void> onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      // Cancel outbound state transfers for destinations that are no longer members in new topology
      // If the rebalance was cancelled, stop every outbound transfer. This will prevent "leaking" transfers
      // from one rebalance to the next.
      Set<Address> members = new HashSet<>(cacheTopology.getWriteConsistentHash().getMembers());
      synchronized (transfersByDestination) {
         for (Iterator<Map.Entry<Address, List<OutboundTransferTask>>> it = transfersByDestination.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Address, List<OutboundTransferTask>> destination = it.next();
            Address address = destination.getKey();
            if (!members.contains(address)) {
               List<OutboundTransferTask> transfers = destination.getValue();
               it.remove();
               for (OutboundTransferTask outboundTransfer : transfers) {
                  outboundTransfer.cancel();
               }
            }
         }
      }
      return CompletableFutures.completedNull();
      //todo [anistor] must cancel transfers for all segments that we no longer own
   }

   // Must start before StateTransferManager sends the join request
   @Start(priority = 50)
   @Override
   public void start() {
      timeout = configuration.clustering().stateTransfer().timeout();
      chunkSize = configuration.clustering().stateTransfer().chunkSize();
   }

   @Stop(priority = 0)
   @Override
   public void stop() {
      if (log.isTraceEnabled()) {
         log.tracef("Shutting down StateProvider of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
      // cancel all outbound transfers
      try {
         synchronized (transfersByDestination) {
            for (Iterator<List<OutboundTransferTask>> it = transfersByDestination.values().iterator(); it.hasNext(); ) {
               List<OutboundTransferTask> transfers = it.next();
               it.remove();
               for (OutboundTransferTask outboundTransfer : transfers) {
                  outboundTransfer.cancel();
               }
            }
         }
      } catch (Throwable t) {
         log.errorf(t, "Failed to stop StateProvider of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
   }

   public CompletionStage<List<TransactionInfo>> getTransactionsForSegments(Address destination, int requestTopologyId,
                                                                            IntSet segments) {
      if (log.isTraceEnabled()) {
         log.tracef("Received request for transactions from node %s for cache %s, topology id %d, segments %s",
                    destination, cacheName, requestTopologyId, segments);
      }

      return getCacheTopology(requestTopologyId, destination, true)
                .thenApply(topology -> {
                   final ConsistentHash readCh = topology.getReadConsistentHash();

                   IntSet ownedSegments = IntSets.from(readCh.getSegmentsForOwner(rpcManager.getAddress()));
                   if (!ownedSegments.containsAll(segments)) {
                      segments.removeAll(ownedSegments);
                      throw new IllegalArgumentException(
                         "Segments " + segments + " are not owned by " + rpcManager.getAddress());
                   }

                   List<TransactionInfo> transactions = new ArrayList<>();
                   //we migrate locks only if the cache is transactional and distributed
                   if (configuration.transaction().transactionMode().isTransactional()) {
                      collectTransactionsToTransfer(destination, transactions, transactionTable.getRemoteTransactions(),
                                                    segments,
                                                    topology);
                      collectTransactionsToTransfer(destination, transactions, transactionTable.getLocalTransactions(),
                                                    segments,
                                                    topology);
                      if (log.isTraceEnabled()) {
                         log.tracef("Found %d transaction(s) to transfer", transactions.size());
                      }
                   }
                   return transactions;
                });
   }

   @Override
   public Collection<ClusterListenerReplicateCallable<Object, Object>> getClusterListenersToInstall() {
      return clusterCacheNotifier.retrieveClusterListenerCallablesToInstall();
   }

   private CompletionStage<CacheTopology> getCacheTopology(int requestTopologyId, Address destination,
                                                           boolean isReqForTransactions) {
      CacheTopology cacheTopology = distributionManager.getCacheTopology();
      int currentTopologyId = cacheTopology.getTopologyId();
      if (requestTopologyId < currentTopologyId) {
         if (isReqForTransactions)
            log.debugf("Transactions were requested by node %s with topology %d, older than the local topology (%d)",
                       destination, requestTopologyId, currentTopologyId);
         else
            log.debugf("Segments were requested by node %s with topology %d, older than the local topology (%d)",
                       destination, requestTopologyId, currentTopologyId);
      } else if (requestTopologyId > currentTopologyId) {
         if (log.isTraceEnabled()) {
            log.tracef("%s were requested by node %s with topology %d, greater than the local " +
                       "topology (%d). Waiting for topology %d to be installed locally.",
                       isReqForTransactions ? "Transactions" : "Segments", destination,
                       requestTopologyId, currentTopologyId, requestTopologyId);
         }
         return stateTransferLock.topologyFuture(requestTopologyId)
                                 .exceptionally(throwable -> {
                                    throw CLUSTER.failedWaitingForTopology(requestTopologyId);
                                 })
                                 .thenApply(ignored -> distributionManager.getCacheTopology());
      }
      return CompletableFuture.completedFuture(cacheTopology);
   }

   private void collectTransactionsToTransfer(Address destination,
                                              List<TransactionInfo> transactionsToTransfer,
                                              Collection<? extends CacheTransaction> transactions,
                                              IntSet segments, CacheTopology cacheTopology) {
      int topologyId = cacheTopology.getTopologyId();
      Set<Address> members = new HashSet<>(cacheTopology.getMembers());

      // no need to filter out state transfer generated transactions because there should not be any such transactions running for any of the requested segments
      for (CacheTransaction tx : transactions) {
         final GlobalTransaction gtx = tx.getGlobalTransaction();
         // Skip transactions whose originators left. The topology id check is needed for joiners.
         // Also skip transactions that originates after state transfer starts.
         if (tx.getTopologyId() == topologyId ||
               (transactionOriginatorChecker.isOriginatorMissing(gtx, members))) {
            if (log.isTraceEnabled()) log.tracef("Skipping transaction %s as it was started in the current topology or by a leaver", tx);
            continue;
         }

         // transfer only locked keys that belong to requested segments
         Set<Object> filteredLockedKeys = new HashSet<>();
         //avoids the warning about synchronizing in a local variable.
         //and allows us to change the CacheTransaction internals without having to worry about it
         Consumer<Object> lockFilter = key -> {
            if (segments.contains(keyPartitioner.getSegment(key))) {
               filteredLockedKeys.add(key);
            }
         };
         tx.forEachLock(lockFilter);
         tx.forEachBackupLock(lockFilter);
         if (filteredLockedKeys.isEmpty()) {
            if (log.isTraceEnabled()) log.tracef("Skipping transaction %s because the state requestor %s doesn't own any key",
                    tx, destination);
            continue;
         }
         if (log.isTraceEnabled()) log.tracef("Sending transaction %s to new owner %s", tx, destination);

         // If a key affected by a local transaction has a new owner, we must add the new owner to the transaction's
         // affected nodes set, so that the it receives the commit/rollback command. See ISPN-3389.
         if(tx instanceof LocalTransaction) {
            LocalTransaction localTx = (LocalTransaction) tx;
            localTx.locksAcquired(Collections.singleton(destination));
            if (log.isTraceEnabled()) log.tracef("Adding affected node %s to transferred transaction %s (keys %s)", destination,
                  gtx, filteredLockedKeys);
         }
         transactionsToTransfer.add(new TransactionInfo(gtx, tx.getTopologyId(), tx.getModifications(),
               filteredLockedKeys));
      }
   }

   @Override
   public void startOutboundTransfer(Address destination, int requestTopologyId, IntSet segments, boolean applyState) {
      if (log.isTraceEnabled()) {
         log.tracef("Starting outbound transfer to node %s for cache %s, topology id %d, segments %s", destination,
                    cacheName, requestTopologyId, segments);
      }

      // the destination node must already have an InboundTransferTask waiting for these segments
      OutboundTransferTask outboundTransfer =
         new OutboundTransferTask(destination, segments, this.configuration.clustering().hash().numSegments(),
                                  chunkSize, requestTopologyId, chunks -> {}, rpcManager,
                                  commandsFactory, timeout, cacheName, applyState);
      addTransfer(outboundTransfer);
      outboundTransfer.execute(readEntries(segments))
                      .whenComplete((ignored, throwable) -> {
                         if (throwable != null) {
                            logError(outboundTransfer, throwable);
                         }
                         onTaskCompletion(outboundTransfer);
                      });
   }

   protected Flowable<SegmentPublisherSupplier.Notification<InternalCacheEntry<?, ?>>> readEntries(IntSet segments) {
      SegmentAwarePublisherSupplier<?> publisher =
            localPublisherManager.entryPublisher(segments, null, null,
                  STATE_TRANSFER_ENTRIES_FLAGS, DeliveryGuarantee.AT_MOST_ONCE, Function.identity());
      return Flowable.fromPublisher(publisher.publisherWithSegments())
            .map(notification -> (SegmentPublisherSupplier.Notification<InternalCacheEntry<?, ?>>) notification);
   }

   protected void addTransfer(OutboundTransferTask transferTask) {
      if (log.isTraceEnabled()) {
         log.tracef("Adding outbound transfer to %s for segments %s", transferTask.getDestination(),
                    transferTask.getSegments());
      }
      synchronized (transfersByDestination) {
         List<OutboundTransferTask> transfers = transfersByDestination
               .computeIfAbsent(transferTask.getDestination(), k -> new ArrayList<>());
         transfers.add(transferTask);
      }
   }

   @Override
   public void cancelOutboundTransfer(Address destination, int topologyId, IntSet segments) {
      if (log.isTraceEnabled()) {
         log.tracef("Cancelling outbound transfer to node %s for cache %s, topology id %d, segments %s", destination,
                    cacheName, topologyId, segments);
      }
      // get the outbound transfers for this address and given segments and cancel the transfers
      synchronized (transfersByDestination) {
         List<OutboundTransferTask> transferTasks = transfersByDestination.get(destination);
         if (transferTasks != null) {
            // get an array copy of the collection to avoid ConcurrentModificationException if the entire task gets cancelled and removeTransfer(transferTask) is called
            OutboundTransferTask[] taskListCopy = transferTasks.toArray(new OutboundTransferTask[0]);
            for (OutboundTransferTask transferTask : taskListCopy) {
               if (transferTask.getTopologyId() == topologyId) {
                  transferTask.cancelSegments(segments); //this can potentially result in a call to removeTransfer(transferTask)
               }
            }
         }
      }
   }

   private void removeTransfer(OutboundTransferTask transferTask) {
      synchronized (transfersByDestination) {
         List<OutboundTransferTask> transferTasks = transfersByDestination.get(transferTask.getDestination());
         if (transferTasks != null) {
            transferTasks.remove(transferTask);
            if (transferTasks.isEmpty()) {
               transfersByDestination.remove(transferTask.getDestination());
            }
         }
      }
   }

   protected void onTaskCompletion(OutboundTransferTask transferTask) {
      if (log.isTraceEnabled()) {
         log.tracef("Removing %s outbound transfer of segments to %s for cache %s, segments %s",
                    transferTask.isCancelled() ? "cancelled" : "completed", transferTask.getDestination(),
                    cacheName, transferTask.getSegments());
      }

      removeTransfer(transferTask);
   }

   protected void logError(OutboundTransferTask task, Throwable t) {
      if (task.isCancelled()) {
         // ignore eventual exceptions caused by cancellation or by the node stopping
         if (log.isTraceEnabled()) {
            log.tracef("Ignoring error in already cancelled transfer to node %s, segments %s",
                       task.getDestination(), task.getSegments());
         }
      } else {
         log.failedOutBoundTransferExecution(t);
      }
   }

   private InternalCacheEntry<Object, Object> defaultMapEntryFromStore(MarshallableEntry<Object, Object> me) {
      InternalCacheEntry<Object, Object> entry = entryFactory.create(me.getKey(), me.getValue(), me.getMetadata());
      entry.setInternalMetadata(me.getInternalMetadata());
      return entry;
   }
}
