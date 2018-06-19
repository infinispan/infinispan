package org.infinispan.remoting.inboundhandler.action;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.concurrent.locks.PendingLockListener;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.PendingLockPromise;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.concurrent.locks.TransactionalRemoteLockCommand;

/**
 * An {@link Action} implementation that check for older topology transactions.
 * <p/>
 * This action is ready when no older topology transactions exists or is canceled when the timeout occurs.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class PendingTxAction extends BaseLockingAction implements PendingLockListener {

   private final PendingLockManager pendingLockManager;
   private final CompletableFuture<Void> notifier;
   private volatile PendingLockPromise pendingLockPromise;

   public PendingTxAction(PendingLockManager pendingLockManager, ClusteringDependentLogic clusteringDependentLogic) {
      super(clusteringDependentLogic);
      this.pendingLockManager = pendingLockManager;
      notifier = new CompletableFuture<>();
   }

   @Override
   protected ActionStatus checking(ActionState state) {
      PendingLockPromise promise = pendingLockPromise;
      if (promise != null && promise.isReady() && cas(InternalState.CHECKING, InternalState.MAKE_READY)) {
         if (promise.hasTimedOut()) {
            cas(InternalState.MAKE_READY, InternalState.CANCELED);
            return ActionStatus.CANCELED;
         } else {
            state.updateTimeout(promise.getRemainingTimeout());
            cas(InternalState.MAKE_READY, InternalState.READY);
            return ActionStatus.READY;
         }
      }
      return ActionStatus.NOT_READY;
   }

   @Override
   protected ActionStatus init(ActionState state) {
      if (!cas(InternalState.INIT, InternalState.CHECKING)) {
         return ActionStatus.NOT_READY; //another thread is making progress
      }
      final TxInvocationContext<?> context = createContext(state);
      if (context == null) {
         //cancel everything. nobody else was able to update the state from checking, so no need to check the CAS
         cas(InternalState.CHECKING, InternalState.CANCELED);
         return ActionStatus.CANCELED;
      }
      final long timeout = state.getTimeout();
      final List<Object> keysToLock = getAndUpdateFilteredKeys(state);

      if (keysToLock.isEmpty()) {
         cas(InternalState.CHECKING, InternalState.READY);
         return ActionStatus.READY;
      }

      //remove backup locks here for the key in which we are going to acquire the "real" locks.
      RemoteLockCommand command = state.getCommand();
      if (command instanceof PrepareCommand && ((PrepareCommand) command).isRetriedCommand()) {
         //we could skip the check if "is-retried". if it is the first time, there is no backup locks acquired in any case.
         //clear the backup locks
         context.getCacheTransaction().cleanupBackupLocks();
         keysToLock.removeAll(context.getLockedKeys());
      }
      if (command instanceof LockControlCommand) {
         //the lock command is only issued if the transaction doesn't have the lock for the keys.
         context.getCacheTransaction().removeBackupLocks(((LockControlCommand) command).getKeys());
         keysToLock.removeAll(context.getLockedKeys());
      }

      PendingLockPromise promise = keysToLock.size() == 1 ?
            pendingLockManager.checkPendingTransactionsForKey(context, keysToLock.get(0), timeout, TimeUnit.MILLISECONDS) :
            pendingLockManager.checkPendingTransactionsForKeys(context, keysToLock, timeout, TimeUnit.MILLISECONDS);

      if (promise.isReady()) {
         //nothing to do. nobody else was able to update the state from checking, so no need to check the CAS
         cas(InternalState.CHECKING, InternalState.READY);
         return ActionStatus.READY;
      }
      pendingLockPromise = promise;
      if (!promise.isReady()) {
         promise.addListener(this);
      }
      return check(state);
   }

   @Override
   public void addListener(ActionListener listener) {
      notifier.thenRun(listener::onComplete);
   }

   @Override
   public void onReady() {
      notifier.complete(null);
   }

   private TxInvocationContext<?> createContext(ActionState state) {
      RemoteLockCommand command = state.getCommand();
      if (command instanceof TransactionalRemoteLockCommand) {
         return ((TransactionalRemoteLockCommand) command).createContext();
      }
      return null;
   }
}
