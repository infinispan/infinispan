package org.infinispan.statetransfer;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link StateProvider} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Listener
public class StateProviderImpl implements StateProvider {

   protected static final Log log = LogFactory.getLog(StateProviderImpl.class);
   protected static final boolean trace = log.isTraceEnabled();

   protected String cacheName;
   private Configuration configuration;
   protected RpcManager rpcManager;
   protected CommandsFactory commandsFactory;
   private ClusterCacheNotifier clusterCacheNotifier;
   private TransactionTable transactionTable;     // optional
   protected DataContainer dataContainer;
   protected PersistenceManager persistenceManager; // optional
   protected ExecutorService executorService;
   protected StateTransferLock stateTransferLock;
   protected InternalEntryFactory entryFactory;
   protected long timeout;
   protected int chunkSize;
   protected KeyPartitioner keyPartitioner;
   protected StateConsumer stateConsumer;
   private TransactionOriginatorChecker transactionOriginatorChecker;

   /**
    * A map that keeps track of current outbound state transfers by destination address. There could be multiple transfers
    * flowing to the same destination (but for different segments) so the values are lists.
    */
   private final Map<Address, List<OutboundTransferTask>> transfersByDestination = new HashMap<>();

   public StateProviderImpl() {
   }

   @Inject
   public void init(Cache cache,
                    @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService, //TODO Use a dedicated ExecutorService
                    Configuration configuration,
                    RpcManager rpcManager,
                    CommandsFactory commandsFactory,
                    ClusterCacheNotifier clusterCacheNotifier,
                    PersistenceManager persistenceManager,
                    DataContainer dataContainer,
                    TransactionTable transactionTable,
                    StateTransferLock stateTransferLock,
                    StateConsumer stateConsumer, InternalEntryFactory entryFactory,
                    KeyPartitioner keyPartitioner,
                    TransactionOriginatorChecker transactionOriginatorChecker) {
      this.cacheName = cache.getName();
      this.executorService = executorService;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.clusterCacheNotifier = clusterCacheNotifier;
      this.persistenceManager = persistenceManager;
      this.dataContainer = dataContainer;
      this.transactionTable = transactionTable;
      this.stateTransferLock = stateTransferLock;
      this.stateConsumer = stateConsumer;
      this.entryFactory = entryFactory;
      this.transactionOriginatorChecker = transactionOriginatorChecker;

      timeout = configuration.clustering().stateTransfer().timeout();

      this.chunkSize = configuration.clustering().stateTransfer().chunkSize();
      this.keyPartitioner = keyPartitioner;
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
         for (Iterator<Address> it = transfersByDestination.keySet().iterator(); it.hasNext(); ) {
            Address destination = it.next();
            if (!members.contains(destination)) {
               List<OutboundTransferTask> transfers = transfersByDestination.get(destination);
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

   @Start(priority = 60)
   @Override
   public void start() {
   }

   @Stop(priority = 0)
   @Override
   public void stop() {
      if (trace) {
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

   public List<TransactionInfo> getTransactionsForSegments(Address destination, int requestTopologyId, Set<Integer> segments) throws InterruptedException {
      if (trace) {
         log.tracef("Received request for transactions from node %s for cache %s, topology id %d, segments %s",
                    destination, cacheName, requestTopologyId, segments);
      }

      final CacheTopology cacheTopology = getCacheTopology(requestTopologyId, destination, true);
      final ConsistentHash readCh = cacheTopology.getReadConsistentHash();

      Set<Integer> ownedSegments = readCh.getSegmentsForOwner(rpcManager.getAddress());
      if (!ownedSegments.containsAll(segments)) {
         segments.removeAll(ownedSegments);
         throw new IllegalArgumentException("Segments " + segments + " are not owned by " + rpcManager.getAddress());
      }

      List<TransactionInfo> transactions = new ArrayList<>();
      //we migrate locks only if the cache is transactional and distributed
      if (configuration.transaction().transactionMode().isTransactional()) {
         collectTransactionsToTransfer(destination, transactions, transactionTable.getRemoteTransactions(), segments, cacheTopology);
         collectTransactionsToTransfer(destination, transactions, transactionTable.getLocalTransactions(), segments, cacheTopology);
         if (trace) {
            log.tracef("Found %d transaction(s) to transfer", transactions.size());
         }
      }
      return transactions;
   }

   @Override
   public Collection<DistributedCallable> getClusterListenersToInstall() {
      return clusterCacheNotifier.retrieveClusterListenerCallablesToInstall();
   }

   private CacheTopology getCacheTopology(int requestTopologyId, Address destination, boolean isReqForTransactions) throws InterruptedException {
      CacheTopology cacheTopology = stateConsumer.getCacheTopology();
      int currentTopologyId = cacheTopology != null ? cacheTopology.getTopologyId() : -1;
      if (requestTopologyId < currentTopologyId) {
         if (isReqForTransactions)
            log.debugf("Transactions were requested by node %s with topology %d, older than the local topology (%d)",
                  destination, requestTopologyId, currentTopologyId);
         else
            log.debugf("Segments were requested by node %s with topology %d, older than the local topology (%d)",
                  destination, requestTopologyId, currentTopologyId);
      } else if (requestTopologyId > currentTopologyId) {
         if (trace) {
            log.tracef("%s were requested by node %s with topology %d, greater than the local " +
                  "topology (%d). Waiting for topology %d to be installed locally.", isReqForTransactions ? "Transactions" : "Segments", destination,
                  requestTopologyId, currentTopologyId, requestTopologyId);
         }
         try {
            stateTransferLock.waitForTopology(requestTopologyId, timeout, TimeUnit.MILLISECONDS);
         } catch (TimeoutException e) {
            throw log.failedWaitingForTopology(requestTopologyId);
         }
         cacheTopology = stateConsumer.getCacheTopology();
      }
      return cacheTopology;
   }

   private void collectTransactionsToTransfer(Address destination,
                                              List<TransactionInfo> transactionsToTransfer,
                                              Collection<? extends CacheTransaction> transactions,
                                              Set<Integer> segments, CacheTopology cacheTopology) {
      int topologyId = cacheTopology.getTopologyId();
      Set<Address> members = new HashSet<>(cacheTopology.getMembers());

      // no need to filter out state transfer generated transactions because there should not be any such transactions running for any of the requested segments
      for (CacheTransaction tx : transactions) {
         final GlobalTransaction gtx = tx.getGlobalTransaction();
         // Skip transactions whose originators left. The topology id check is needed for joiners.
         // Also skip transactions that originates after state transfer starts.
         if (tx.getTopologyId() == topologyId ||
               (transactionOriginatorChecker.isOriginatorMissing(gtx, members))) {
            if (trace) log.tracef("Skipping transaction %s as it was started in the current topology or by a leaver", tx);
            continue;
         }

         // transfer only locked keys that belong to requested segments
         Set<Object> filteredLockedKeys = new HashSet<>();
         Set<Object> lockedKeys = tx.getLockedKeys();
         synchronized (lockedKeys) {
            for (Object key : lockedKeys) {
               if (segments.contains(keyPartitioner.getSegment(key))) {
                  filteredLockedKeys.add(key);
               }
            }
         }
         Set<Object> backupLockedKeys = tx.getBackupLockedKeys();
         synchronized (backupLockedKeys) {
            for (Object key : backupLockedKeys) {
               if (segments.contains(keyPartitioner.getSegment(key))) {
                  filteredLockedKeys.add(key);
               }
            }
         }
         if (filteredLockedKeys.isEmpty()) {
            if (trace) log.tracef("Skipping transaction %s because the state requestor %s doesn't own any key",
                    tx, destination);
            continue;
         }
         if (trace) log.tracef("Sending transaction %s to new owner %s", tx, destination);
         List<WriteCommand> txModifications = tx.getModifications();
         WriteCommand[] modifications = null;
         if (!txModifications.isEmpty()) {
            modifications = txModifications.toArray(new WriteCommand[txModifications.size()]);
         }

         // If a key affected by a local transaction has a new owner, we must add the new owner to the transaction's
         // affected nodes set, so that the it receives the commit/rollback command. See ISPN-3389.
         if(tx instanceof LocalTransaction) {
            LocalTransaction localTx = (LocalTransaction) tx;
            localTx.locksAcquired(Collections.singleton(destination));
            if (trace) log.tracef("Adding affected node %s to transferred transaction %s (keys %s)", destination,
                  gtx, filteredLockedKeys);
         }
         transactionsToTransfer.add(new TransactionInfo(gtx, tx.getTopologyId(),
               modifications, filteredLockedKeys));
      }
   }

   @Override
   public void startOutboundTransfer(Address destination, int requestTopologyId, Set<Integer> segments, boolean applyState)
         throws InterruptedException {
      if (trace) {
         log.tracef("Starting outbound transfer to node %s for cache %s, topology id %d, segments %s", destination,
                    cacheName, requestTopologyId, segments);
      }

      // the destination node must already have an InboundTransferTask waiting for these segments
      OutboundTransferTask outboundTransfer = new OutboundTransferTask(destination, segments, chunkSize, requestTopologyId,
            keyPartitioner, this::onTaskCompletion, chunks -> {},
            OutboundTransferTask::defaultMapEntryFromDataContainer, OutboundTransferTask::defaultMapEntryFromStore,
            dataContainer, persistenceManager, rpcManager, commandsFactory, entryFactory, timeout, cacheName, applyState, false);
      addTransfer(outboundTransfer);
      outboundTransfer.execute(executorService);
   }

   protected void addTransfer(OutboundTransferTask transferTask) {
      if (trace) {
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
   public void cancelOutboundTransfer(Address destination, int topologyId, Set<Integer> segments) {
      if (trace) {
         log.tracef("Cancelling outbound transfer to node %s for cache %s, topology id %d, segments %s", destination,
                    cacheName, topologyId, segments);
      }
      // get the outbound transfers for this address and given segments and cancel the transfers
      synchronized (transfersByDestination) {
         List<OutboundTransferTask> transferTasks = transfersByDestination.get(destination);
         if (transferTasks != null) {
            // get an array copy of the collection to avoid ConcurrentModificationException if the entire task gets cancelled and removeTransfer(transferTask) is called
            OutboundTransferTask[] taskListCopy = transferTasks.toArray(new OutboundTransferTask[transferTasks.size()]);
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
      if (trace) {
         log.tracef("Removing %s outbound transfer of segments to %s for cache %s, segments %s",
                    transferTask.isCancelled() ? "cancelled" : "completed", transferTask.getDestination(),
                    cacheName, transferTask.getSegments());
      }

      removeTransfer(transferTask);
   }
}
