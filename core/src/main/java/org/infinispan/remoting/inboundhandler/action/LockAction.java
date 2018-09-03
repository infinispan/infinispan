package org.infinispan.remoting.inboundhandler.action;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.util.concurrent.locks.LockListener;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.concurrent.locks.TransactionalRemoteLockCommand;

/**
 * An {@link Action} implementation that acquire the locks.
 * <p/>
 * It returns {@link ActionStatus#READY} when the locks are available to acquired or the acquisition failed (timeout or
 * deadlock).
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class LockAction extends BaseLockingAction implements LockListener {

   private final LockManager lockManager;
   private final CompletableFuture<Void> notifier;
   private volatile LockPromise lockPromise;

   public LockAction(LockManager lockManager, DistributionManager distributionManager) {
      super(distributionManager);
      this.lockManager = lockManager;
      notifier = new CompletableFuture<>();
   }

   @Override
   protected ActionStatus checking(ActionState ignored) {
      LockPromise promise = lockPromise;
      if (promise != null && promise.isAvailable()) {
         cas(InternalState.CHECKING, InternalState.READY);
         return ActionStatus.READY;
      } else {
         return ActionStatus.NOT_READY;
      }
   }

   @Override
   protected ActionStatus init(ActionState state) {
      if (!cas(InternalState.INIT, InternalState.CHECKING)) {
         return ActionStatus.NOT_READY; //another thread is making progress
      }
      final Object lockOwner = getLockOwner(state);
      final long timeout = state.getTimeout();
      List<Object> keysToLock = getAndUpdateFilteredKeys(state);

      if (keysToLock.isEmpty()) {
         return cas(InternalState.CHECKING, InternalState.READY) ? ActionStatus.READY : ActionStatus.NOT_READY;
      }

      TxInvocationContext context = createContext(state);
      if (context != null) {
         keysToLock.forEach(context::addLockedKey);
      }

      LockPromise promise = keysToLock.size() == 1 ?
            lockManager.lock(keysToLock.get(0), lockOwner, timeout, TimeUnit.MILLISECONDS) :
            lockManager.lockAll(keysToLock, lockOwner, timeout, TimeUnit.MILLISECONDS);

      lockPromise = promise;
      if (!promise.isAvailable()) {
         promise.addListener(this);
      }
      return check(state);
   }

   private Object getLockOwner(ActionState state) {
      RemoteLockCommand command = state.getCommand();
      return command instanceof TransactionalRemoteLockCommand ?
            ((TransactionalRemoteLockCommand) command).createContext().getLockOwner() :
            command.getKeyLockOwner();
   }

   @Override
   public void addListener(ActionListener listener) {
      notifier.thenRun(listener::onComplete);
   }

   @Override
   public void onException(ActionState state) {
      final Object lockOwner = getLockOwner(state);
      List<Object> keysToLock = getAndUpdateFilteredKeys(state);
      lockManager.unlockAll(keysToLock, lockOwner);
   }

   @Override
   public void onEvent(LockState state) {
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
