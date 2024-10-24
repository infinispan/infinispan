package org.infinispan.util.concurrent.locks.deadlock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.DeadlockDetection;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.impl.LockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import jakarta.transaction.InvalidTransactionException;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/**
 * This class implements an algorithm for distributed deadlock detection and resolution.
 *
 * <p>
 * The algorithm is the Chandy-Misra-Haas (CMH) Algorithm [1] with optimizations by Roesler et al. [2]. CMH is an
 * edge-chasing algorithm for an AND resource model. Edge-chasing means we create the dependency graph on demand by utilizing
 * probing commands. An AND resource model means the nodes must acquire ownership of ALL resources to proceed, e.g.,
 * lock all keys before proceeding with a transaction. Lastly, the optimizations by Roesler et al. [2] utilize an ordering
 * mechanism to ensure only the necessary resources are released to guarantee progress. The original CMH could end on
 * a live lock by rolling back all the transactions in a cycle.
 * </p>
 *
 * <p>
 * Oversimplifying, the algorithm starts when a lock is not immediately acquired. The initiating transaction sends a
 * probe command ({@link DeadlockProbeCommand}) to the transaction holding the resource. Upon receiving a probe, the
 * node verifies which resources the transaction is waiting for and relays the probe command with the initial originating
 * transaction to the nodes holding the resources. This relay process continues until a node receives the probe where the
 * initiator is the same as the holder. This process concludes the deadlock <b>detection</b> phase.
 * </p>
 *
 * <p>
 * The deadlock <b>resolution</b> involves breaking the cycle. Upon receiving a probe identifying the cycle, the node
 * aborts the local transaction. Since the node owns the resource's holder, the transaction must exist locally. The node
 * marks the transaction to rollback, moves the local locks ({@link org.infinispan.util.concurrent.locks.impl.InfinispanLock})
 * to a deadlock state ({@link org.infinispan.util.concurrent.locks.LockState#DEADLOCKED}), and notifies the remote nodes
 * about the deadlock. The remote nodes move the locks to a deadlock state, which returns any pending request of
 * the {@link org.infinispan.commands.control.LockControlCommand}.
 * </p>
 *
 * <p>
 * The optimization to avoid aborting too many transactions restricts the probing initialization. An initiator only probes
 * a holder when the initiator is smaller (more recent) than the holder. This approach aims to avoid aborting older
 * transactions, reasoning that older transactions are more likely to acquire all the resources to proceed.
 * </p>
 *
 * <h3>Network Partitions</h3>
 *
 * <p>
 * Since this is an edge-chasing algorithm, it detects cycles when nodes have a reliable communication channel. In the
 * case of network partitions, the algorithm does not detect cycles since the nodes across partitions can't communicate.
 * However, the algorithm identifies cycles within a single partition, even in degraded mode.
 * </p>
 *
 * <p>
 * Upon partition healing, the algorithm sends probes for local transactions depending on remote resources. This approach
 * ensures the algorithm eventually identifies the cycles after nodes can communicate. However, the algorithm may abort
 * more transactions than necessary during network partition and healing.
 * </p>
 *
 * @see <a href="https://doi.org/10.1145/357360.357365">[1] Chandy-Misra-Haas Algorithm</a>
 * @see <a href="https://doi.org/10.1109/12.30874">[2] M. Roesler; W.A. Burkhard</a>
 * @see <a href="https://github.com/infinispan/infinispan-designs/blob/main/deadlock-detection-resolution.md">Design proposal</a>
 * @author José Bolina
 */
@Scope(Scopes.NAMED_CACHE)
final class DistributedDeadlockDetection implements DeadlockDetection {

   private static final Log log = LogFactory.getLog(DistributedDeadlockDetection.class);

   private final AtomicLong piggyback = new AtomicLong(0L);
   private ProbeExecutorHelper executor;
   private DeadlockDetectionViewListener viewListener;

   @Inject Configuration configuration;
   @Inject LockContainer lockContainer;
   @Inject ComponentRegistry registry;
   @Inject BlockingManager blockingManager;
   @Inject TransactionManager tm;
   @Inject CommandsFactory factory;
   @Inject DistributionManager distributionManager;
   @Inject CacheManagerNotifier notifier;
   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject String cacheName;

   @Start
   void start() {
      executor = new ProbeExecutorHelper(registry);
      viewListener = new DeadlockDetectionViewListener(this);
      notifier.addListener(viewListener);
   }

   @Stop
   void stop() {
      notifier.removeListener(viewListener);
   }

   /**
    * Asynchronously sends the probe for the initiator to the holder.
    *
    * <p>
    * The algorithm only starts when the initiator is more recent than the holder. Both objects must be transactions.
    * </p>
    *
    * @param initiator: The initiator object trying to acquire locks.
    * @param holder: The object currently holding the resource.
    */
   @Override
   public void initializeDeadlockDetection(Object initiator, Object holder) {
      if (!(initiator instanceof GlobalTransaction gi)) return;
      if (!(holder instanceof GlobalTransaction gh)) return;

      // Restrict the probes to only start when the initiator is more recent than the resource holder.
      // This should provide the means for the older transactions to complete while aborting the more recent.
      if (gi.compareTo(gh) < 0) {
         log.tracef("%s do not probe %s", gi, gh);
         return;
      }

      log.debugf("Starting deadlock detection: h=%s; i=%s", gh, gi);
      probeTransaction(gi, gh);
   }

   /**
    * Process the probing command.
    *
    * <p>
    * Verifies if there is a cycle or relays the probe to the respective owners to continue the deadlock detection.
    * In case of a cycle, abort the transaction, mark the locks to deadlock state, and probe the remaining owners.
    * </p>
    *
    * @param i: The probe command initiator.
    * @param h: The current resource owner.
    * @param keys: The keys in the transaction context ({@link DeadlockProbeCommand#getGlobalTransaction()}).
    * @return A completable future that finishes after handling the command.
    */
   @Override
   public CompletionStage<Void> verifyDeadlockCycle(Object i, Object h, Collection<?> keys) {
      if (!(i instanceof GlobalTransaction initiator)) return CompletableFutures.completedNull();
      if (!(h instanceof GlobalTransaction holder)) return CompletableFutures.completedNull();

      // We have a deadlock when the initiator and holder are equal.
      // We trigger a rollback for the transaction to release the keys on the involved nodes.
      // A local deadlock also needs to abort the transaction.
      if (Objects.equals(initiator, holder) || isLocalDeadlock(initiator, holder)) {
         return CompletionStages.handleAndCompose(tryRollback(initiator), (ignore, t) -> {
            if (t != null) log.errorf(t, "Failed rolling back: %s", initiator);

            return deadlockDetected(initiator, keys);
         });
      }

      // If the holder has completed, no need to continue.
      if (isTransactionComplete(holder))
         return CompletableFutures.completedNull();

      // No deadlock, we need to probe all the nodes involved in the holder's transaction.
      // We iterate over all affected keys to probe the holders.
      return probeLockOwners(initiator, holder, keys);
   }

   @Override
   public boolean isEnabled() {
      return true;
   }

   /**
    * Start probing local transactions missing locks.
    *
    * <p>
    * Iterate over all local transactions that haven't acquired all locks. For each transaction, send a probe to the
    * node holding the lock to the keys.
    * </p>
    *
    * @return A completable future that finishes after sending the remote probes.
    */
   CompletionStage<Void> probeAllLocalTransactions() {
      if (piggyback.getAndIncrement() > 0)
         return CompletableFutures.completedNull();

      TransactionTable table = getTransactionTable();
      Collection<LocalTransaction> transactions = table.getLocalTransactions();
      if (log.isTraceEnabled()) {
         log.tracef("inspecting all %d local transactions", transactions.size());
      }

      AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();
      for (LocalTransaction ct : transactions) {
         Set<?> keys = ct.getInspectedKeys();

         // Only proceed if the locked keys is missing an inspected key.
         // That is, the transaction is still missing lock ownership of a key.
         if (!isTransactionComplete(ct.getGlobalTransaction()) && !ct.getLockedKeys().containsAll(keys)) {
            acs.dependsOn(probeTransactionsInKeys(ct.getGlobalTransaction(), keys));
         }
      }

      return CompletionStages.handleAndCompose(acs.freeze(), (ignore, t) -> {
         if (piggyback.getAndSet(0) > 1) {
            return probeAllLocalTransactions();
         }
         return CompletableFutures.completedNull();
      });
   }

   private CompletionStage<Void> probeTransactionsInKeys(GlobalTransaction initiator, Collection<?> keys) {
      AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();
      LockManager lockManager = getLockManager();
      Set<Object> probe = new HashSet<>();
      for (Object key : keys) {
         Object owner = lockManager.getOwner(key);
         if (!(owner instanceof GlobalTransaction go)) continue;
         if (initiator.compareTo(go) <= 0) continue;
         if (!probe.add(owner)) continue;

         acs.dependsOn(probeTransaction(initiator, go));
      }

      return acs.freeze();
   }

   /**
    * Checks for a local deadlock between the transactions.
    *
    * <p>
    * This might happen in a cycle where the node is waiting on another local transaction. We check that both transactions
    * are local and the inspected keys during locks overlap.
    * </p>
    *
    * @param i: The transaction initiator.
    * @param h: The resource holder.
    * @return <code>true</code> if there is a local deadlock, <code>false</code>, otherwise.
    */
   private boolean isLocalDeadlock(GlobalTransaction i, GlobalTransaction h) {
      TransactionTable table = getTransactionTable();
      LocalTransaction initiator = table.getLocalTransaction(i);
      LocalTransaction holder = table.getLocalTransaction(h);

      if (initiator == null || holder == null) return false;
      return !Collections.disjoint(initiator.getInspectedKeys(), holder.getInspectedKeys());
   }

   private CompletionStage<Void> tryRollback(GlobalTransaction gtx) {
      if (isTransactionComplete(gtx)) return CompletableFutures.completedNull();

      TransactionTable table = getTransactionTable();

      // Mark remote as deadlock to hint in case the LockControlCommand is running concurrently.
      RemoteTransaction rt = table.getRemoteTransaction(gtx);
      if (rt != null) rt.markAsDeadlock();

      LocalTransaction tx = table.getLocalTransaction(gtx);
      if (tx == null || !tx.isActive()) return CompletableFutures.completedNull();

      return blockingManager.runBlocking(() -> {
         Transaction existing = null;
         try {
            boolean resume = false;
            existing = tm.getTransaction();
            if (existing != null) {
               if (!Objects.equals(existing, tx.getTransaction())) {
                  existing = tm.suspend();
                  resume = true;
               }
            } else {
               resume = true;
            }

            if (resume) tm.resume(tx.getTransaction());
            tm.setRollbackOnly();
            log.deadlockIdentified(gtx);
         } catch (SystemException | InvalidTransactionException e) {
            log.errorf(e, "Failed rolling back deadlocked tx: %s", gtx);
         } finally {
            if (existing != null) {
               resume(tm, existing);
            }
         }
      }, "deadlock-rollback-" + gtx.globalId());
   }

   private void resume(TransactionManager tm, Transaction t) {
      try {
         tm.suspend();
         tm.resume(t);
      } catch (InvalidTransactionException | SystemException e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Identified a deadlock cycle.
    *
    * <p>
    * Move every local lock where the transaction is currently waiting to a {@link org.infinispan.util.concurrent.locks.LockState#DEADLOCKED}.
    * Then, if the node is owner of the transaction, notify the remote nodes about the deadlock.
    * </p>
    *
    * @param gtx: Transaction in the deadlock cycle to abort.
    * @param keys: The keys the transaction is operating.
    * @return A completable future that finishes after notifying remote nodes.
    */
   private CompletionStage<Void> deadlockDetected(GlobalTransaction gtx, Collection<?> keys) {
      // Mark the pending locks as deadlock.
      lockContainer.deadlockCheck((pending, current) -> Objects.equals(pending, gtx));

      // Complete the pending futures exceptionally with a deadlock exception.
      PendingLockManager plm = getPendingLockManager();
      plm.onPendingPromises(plp -> {
         if (Objects.equals(plp.keyOwner(), gtx)) {
            plp.completeExceptionally(new DeadlockDetectedException("Deadlock detected"));
         }
      });

      // Lastly, probe any remote locks depending on the rolled transaction.
      return deadlockRemoteLocks(gtx, keys);
   }

   /**
    * Probe the current key owners.
    *
    * <p>
    * Check the current owners of the keys and relay the probe message containing the original initiator. If necessary,
    * include the local transactions which overlaps the key set and is pending lock.
    * </p>
    *
    * @param initiator: The transaction initiating the probing mechanism.
    * @param holder: The current holder received by the probe.
    * @param keys: The keys the holder transaction is operating.
    * @return A completable future that completes after sending the remote probes.
    */
   private CompletionStage<Void> probeLockOwners(GlobalTransaction initiator, GlobalTransaction holder, Collection<?> keys) {
      log.debugf("Probing %d keys on %s for pending %s", keys.size(), holder, initiator);

      // First, analyze transactions currently holding the locks.
      Set<GlobalTransaction> probed = new HashSet<>();
      AggregateCompletionStage<Void> acs = CompletionStages.aggregateCompletionStage();
      LockManager lm = getLockManager();
      for (Object key : keys) {
         Object owner = lm.getOwner(key);
         if (!(owner instanceof GlobalTransaction gt)) continue;
         if (Objects.equals(gt, holder)) continue;
         if (!probed.add(gt)) continue;

         acs.dependsOn(probeTransaction(initiator, gt));
      }

      // We piggyback the local transactions as well. We include local transaction which are missing keys and work over
      // an overlapping key set.
      TransactionTable table = getTransactionTable();
      for (LocalTransaction lt : table.getLocalTransactions()) {
         GlobalTransaction localGtx = lt.getGlobalTransaction();
         Set<Object> acquired = lt.getLockedKeys();

         // If there is a local transaction, not holding the local locks, we piggyback the probing mechanism.
         // The TX is still missing locks, and it operates over the same key set as the receiving probe.
         if (!Collections.disjoint(keys, lt.getInspectedKeys()) && !acquired.containsAll(lt.getInspectedKeys()) && probed.add(localGtx)) {
            // If the local transaction hasn't acquired all keys and has an overlapping set with the initiator.
            // We send a deadlock back.
            if (Objects.equals(holder, localGtx) && isLocalWaiting(holder)) {
               acs.dependsOn(probeTransaction(initiator, initiator));
            } else {
               // We reverse the initiator, otherwise, we have an infinite loop sending to the local node.
               // Since a local transaction has pending transactions and share keys as the remote, we start probing
               // for the local transactions to proceed and identify any cycle.
               acs.dependsOn(probeTransaction(localGtx, initiator));
            }
         }
      }

      return acs.freeze();
   }

   private boolean isLocalWaiting(GlobalTransaction gtx) {
      TransactionTable table = getTransactionTable();
      LocalTransaction local = table.getLocalTransaction(gtx);
      return local != null;
   }

   private boolean isTransactionComplete(GlobalTransaction gtx) {
      TransactionTable table = getTransactionTable();
      if (table.isTransactionCompleted(gtx))
         return true;

      LocalTransaction local = table.getLocalTransaction(gtx);
      if (local != null)
         return local.isMarkedForRollback();

      RemoteTransaction remote = table.getRemoteTransaction(gtx);
      if (remote != null)
         return remote.hasReceivedDeadlock();

      return false;
   }

   private TransactionTable getTransactionTable() {
      return registry.getTransactionTable();
   }

   private LockManager getLockManager() {
      return registry.getLockManager().running();
   }

   private PendingLockManager getPendingLockManager() {
      return ComponentRegistry.componentOf(registry.getCache().running(), PendingLockManager.class);
   }

   private CompletionStage<Void> deadlockRemoteLocks(GlobalTransaction gtx, Collection<?> keys) {
      TransactionTable table = getTransactionTable();
      LocalTransaction transaction = table.getLocalTransaction(gtx);

      // Only probe remote if the node owns the transaction.
      if (transaction == null) return CompletableFutures.completedNull();

      // Identify every node affected by the transaction.
      // Send the deadlock command to all nodes to move the pending locks to a deadlock state. Make sure to include
      // any new nodes from topology changes.
      Collection<Address> targets = getRemoteDeadlockTargets(transaction, keys);
      return executor.execute(targets, createProbeCommand(gtx, gtx));
   }

   private Collection<Address> getRemoteDeadlockTargets(LocalTransaction tx, Collection<?> keys) {
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();
      Collection<Address> nodes = isReplicated()
            ? topology.getMembersSet()
            : topology.getWriteOwners(keys);
      // Include every possible node required in the transaction.
      return tx.getCommitNodes(nodes, topology);
   }

   private boolean isReplicated() {
      return configuration.clustering().cacheMode().isReplicated();
   }

   private CompletionStage<Void> probeTransaction(GlobalTransaction initiator, GlobalTransaction holder) {
      return probeTransaction(holder.getAddress(), initiator, holder);
   }

   private CompletionStage<Void> probeTransaction(Address address, GlobalTransaction initiator, GlobalTransaction holder) {
      log.tracef("Inspecting (i=%s) -> (h=%s)", initiator, holder);
      return executor.execute(address, createProbeCommand(initiator, holder));
   }

   private DeadlockProbeCommand createProbeCommand(GlobalTransaction initiator, GlobalTransaction holder) {
      DeadlockProbeCommand command = factory.buildDeadlockProbeCommand(initiator, holder);
      command.setTopologyId(distributionManager.getCacheTopology().getTopologyId());
      return command;
   }
}
