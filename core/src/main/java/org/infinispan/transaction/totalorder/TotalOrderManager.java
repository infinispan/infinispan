package org.infinispan.transaction.totalorder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.impl.TotalOrderRemoteTransactionState;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class behaves as a synchronization point between incoming transactions (totally ordered) and between incoming
 * transactions and state transfer.
 * <p>
 * Main functions:
 * <ul>
 *    <li>
 *       ensure an order between prepares before sending them to the thread pool, i.e. non-conflicting
 * prepares can be processed concurrently;
 *    </li>
 *    <li>
 *       ensure that the state transfer waits for the previous delivered prepares;
 *    </li>
 *    <li>
 *       ensure that the prepare waits for state transfer in progress.
 *    </li>
 * </ul>
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderManager {

   private static final Log log = LogFactory.getLog(TotalOrderManager.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject @ComponentName(KnownComponentNames.REMOTE_COMMAND_EXECUTOR)
   private BlockingTaskAwareExecutorService totalOrderExecutor;

   /**
    * this map is used to keep track of concurrent transactions.
    */
   private final ConcurrentMap<Object, TotalOrderLatch> keysLocked;
   private final AtomicReference<TotalOrderLatch> stateTransferInProgress;

   public TotalOrderManager() {
      keysLocked = CollectionFactory.makeConcurrentMap();
      stateTransferInProgress = new AtomicReference<>(null);
   }

   /**
    * It ensures the validation order for the transaction corresponding to the prepare command. This allow the prepare
    * command to be moved to a thread pool.
    *
    * @param state the total order prepare state
    */
   public final void ensureOrder(TotalOrderRemoteTransactionState state, Collection<?> keysModified) throws InterruptedException {
      //the retries due to state transfer re-uses the same state. we need that the keys previous locked to be release
      //in order to insert it again in the keys locked.
      //NOTE: this method does not need to be synchronized because it is invoked by a one thread at the time, namely
      //the thread that is delivering the messages in total order.
      state.awaitUntilReset();
      TotalOrderLatch transactionSynchronizedBlock = new TotalOrderLatchImpl(state.getGlobalTransaction().globalId());
      state.setTransactionSynchronizedBlock(transactionSynchronizedBlock);
         //this will collect all the count down latch corresponding to the previous transactions in the queue
         for (Object key : keysModified) {
            TotalOrderLatch prevTx = keysLocked.put(key, transactionSynchronizedBlock);
            if (prevTx != null) {
               state.addSynchronizedBlock(prevTx);
            }
            state.addLockedKey(key);

      }

      TotalOrderLatch stateTransfer = stateTransferInProgress.get();
      if (stateTransfer != null) {
         state.addSynchronizedBlock(stateTransfer);
      }

      if (trace) {
         log.tracef("Transaction [%s] will wait for %s and locked %s", state.getGlobalTransaction().globalId(),
                    state.getConflictingTransactionBlocks(), state.getLockedKeys() == null ? "[ClearCommand]" :
               state.getLockedKeys());
      }
   }

   /**
    * Release the locked key possibly unblock waiting prepares.
    *
    * @param state the state
    */
   public final void release(TotalOrderRemoteTransactionState state) {
      TotalOrderLatch synchronizedBlock = state.getTransactionSynchronizedBlock();
      if (synchronizedBlock == null) {
         //already released!
         return;
      }
      Collection<Object> lockedKeys = state.getLockedKeys();
      synchronizedBlock.unBlock();
         for (Object key : lockedKeys) {
            keysLocked.remove(key, synchronizedBlock);
         }
      if (trace) {
         log.tracef("Release %s and locked keys %s. Checking pending tasks!", synchronizedBlock, lockedKeys);
      }
      state.reset();
   }

   /**
    * It notifies that a state transfer is about to start.
    *
    * @param topologyId the new topology ID
    * @return the current pending prepares
    */
   public final Collection<TotalOrderLatch> notifyStateTransferStart(int topologyId, boolean isRebalance) {
      if (stateTransferInProgress.get() != null) {
         return Collections.emptyList();
      }
      List<TotalOrderLatch> preparingTransactions = new ArrayList<>(keysLocked.size());
      preparingTransactions.addAll(keysLocked.values());
      if (isRebalance) {
         stateTransferInProgress.set(new TotalOrderLatchImpl("StateTransfer-" + topologyId));
      }
      if (trace) {
         log.tracef("State Transfer start. It will wait for %s", preparingTransactions);
      }
      return preparingTransactions;
   }

   /**
    * It notifies the end of the state transfer possibly unblock waiting prepares.
    */
   public final void notifyStateTransferEnd() {
      TotalOrderLatch block = stateTransferInProgress.getAndSet(null);
      if (block != null) {
         block.unBlock();
      }
      if (trace) {
         log.tracef("State Transfer finish. It will release %s", block);
      }
      totalOrderExecutor.checkForReadyTasks();
   }

   public final boolean hasAnyLockAcquired() {
      return !keysLocked.isEmpty();
   }
}
