package org.infinispan.interceptors.locking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.PendingLockPromise;

/**
 * Base class for transaction based locking interceptors.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class AbstractTxLockingInterceptor extends AbstractLockingInterceptor {
   @Inject PartitionHandlingManager partitionHandlingManager;
   @Inject PendingLockManager pendingLockManager;

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, unlockAllReturnHandler);
   }

   @Override
   protected Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) {
      if (ctx.isInTxScope())
         return invokeNext(ctx, command);

      return invokeNextAndFinally(ctx, command, unlockAllReturnHandler);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         if (t instanceof OutdatedTopologyException)
            throw t;

         releaseLockOnTxCompletion(((TxInvocationContext<?>) rCtx));
      });
   }

   /**
    * The backup (non-primary) owners keep a "backup lock" for each key they received in a lock/prepare command.
    * Normally there can be many transactions holding the backup lock at the same time, but when the secondary owner
    * becomes a primary owner a new transaction trying to obtain the "real" lock will have to wait for all backup
    * locks to be released. The backup lock will be released either by a commit/rollback/unlock command or by
    * the originator leaving the cluster (if recovery is disabled).
    */
   final InvocationStage lockOrRegisterBackupLock(TxInvocationContext<?> ctx, VisitableCommand command, Object key,
                                                  long lockTimeout) {
      switch (cdl.getCacheTopology().getDistribution(key).writeOwnership()) {
         case PRIMARY:
            return checkPendingAndLockKey(ctx, command, key, lockTimeout);
         case BACKUP:
            ctx.getCacheTransaction().addBackupLockForKey(key);
            // fallthrough
         default:
            return InvocationStage.completedNullStage();
      }
   }

   /**
    * Same as {@link #lockOrRegisterBackupLock(TxInvocationContext, VisitableCommand, Object, long)}
    *
    * @return a collection with the keys locked.
    */
   final InvocationStage lockAllOrRegisterBackupLock(TxInvocationContext<?> ctx, VisitableCommand command,
                                                     Collection<?> keys, long lockTimeout) {
      if (keys.isEmpty()) {
         return InvocationStage.completedNullStage();
      }

      Collection<Object> keysToLock = new ArrayList<>(keys.size());
      AbstractCacheTransaction cacheTransaction = ctx.getCacheTransaction();
      LocalizedCacheTopology cacheTopology = cdl.getCacheTopology();
      for (Object key : keys) {
         // Skip keys that are already locked (when retrying a lock/prepare command)
         if (cacheTransaction.ownsLock(key))
            continue;

         switch (cacheTopology.getDistribution(key).writeOwnership()) {
            case PRIMARY:
               keysToLock.add(key);
               break;
            case BACKUP:
               cacheTransaction.addBackupLockForKey(key);
               break;
            default:
               break;
         }
      }

      if (keysToLock.isEmpty()) {
         return InvocationStage.completedNullStage();
      }

      return checkPendingAndLockAllKeys(ctx, command, keysToLock, lockTimeout);
   }

   /**
    * Besides acquiring a lock, this method also handles the following situation:
    * 1. consistentHash("k") == {A, B}, tx1 prepared on A and B. Then node A crashed (A  == single lock owner)
    * 2. at this point tx2 which also writes "k" tries to prepare on B.
    * 3. tx2 has to determine that "k" is already locked by another tx (i.e. tx1) and it has to wait for that tx to finish before acquiring the lock.
    *
    * The algorithm used at step 3 is:
    * - the transaction table(TT) associates the current topology id with every remote and local transaction it creates
    * - TT also keeps track of the minimal value of all the topology ids of all the transactions still present in the cache (minTopologyId)
    * - when a tx wants to acquire lock "k":
    *    - if tx.topologyId > TT.minTopologyId then "k" might be a key whose owner crashed. If so:
    *       - obtain the list LT of transactions that started in a previous topology (txTable.getTransactionsPreparedBefore)
    *       - for each t in LT:
    *          - if t wants to write "k" then block until t finishes (CacheTransaction.waitForTransactionsToFinishIfItWritesToKey)
    *       - only then try to acquire lock on "k"
    *    - if tx.topologyId == TT.minTopologyId try to acquire lock straight away.
    *
    * Note: The algorithm described below only when nodes leave the cluster, so it doesn't add a performance burden
    * when the cluster is stable.
    */
   private InvocationStage checkPendingAndLockKey(TxInvocationContext<?> ctx, VisitableCommand command, Object key,
                                                  long lockTimeout) {
      PendingLockPromise pendingLockPromise =
         pendingLockManager.checkPendingTransactionsForKey(ctx, key, lockTimeout, TimeUnit.MILLISECONDS);
      if (pendingLockPromise.isReady()) {
         //if it has already timed-out, do not try to acquire the lock
         return pendingLockPromise.hasTimedOut() ?
               pendingLockPromise.toInvocationStage() :
               lockAndRecord(ctx, command, key, lockTimeout);
      }

      return pendingLockPromise.toInvocationStage().thenApplyMakeStage(ctx, command, (rCtx, rCommand, rv) -> {
         long remaining = pendingLockPromise.getRemainingTimeout();
         return lockAndRecord(ctx, command, key, remaining);
      });
   }

   private InvocationStage checkPendingAndLockAllKeys(TxInvocationContext<?> ctx, VisitableCommand command,
                                                      Collection<Object> keys, long lockTimeout) {
      PendingLockPromise pendingLockPromise =
         pendingLockManager.checkPendingTransactionsForKeys(ctx, keys, lockTimeout, TimeUnit.MILLISECONDS);
      if (pendingLockPromise.isReady()) {
         //if it has already timed-out, do not try to acquire the lock
         return pendingLockPromise.hasTimedOut() ?
               pendingLockPromise.toInvocationStage() :
               lockAllAndRecord(ctx, command, keys, lockTimeout);
      }

      return pendingLockPromise.toInvocationStage().thenApplyMakeStage(ctx, command, ((rCtx, rCommand, rv) -> {
         long remaining = pendingLockPromise.getRemainingTimeout();
         return lockAllAndRecord(ctx, command, keys, remaining);
      }));
   }

   void releaseLockOnTxCompletion(TxInvocationContext<?> ctx) {
      boolean shouldReleaseLocks = ctx.isOriginLocal() &&
            !partitionHandlingManager.isTransactionPartiallyCommitted(ctx.getGlobalTransaction());
      if (shouldReleaseLocks) {
         lockManager.unlockAll(ctx);
      }
   }
}
