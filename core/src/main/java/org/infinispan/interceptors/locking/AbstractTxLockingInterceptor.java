package org.infinispan.interceptors.locking;

import static org.infinispan.commons.util.Util.toStr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.logging.Log;

/**
 * Base class for transaction based locking interceptors.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class AbstractTxLockingInterceptor extends AbstractLockingInterceptor {
   protected final boolean trace = getLog().isTraceEnabled();

   @Inject protected RpcManager rpcManager;
   @Inject private PartitionHandlingManager partitionHandlingManager;
   @Inject private PendingLockManager pendingLockManager;

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, unlockAllReturnHandler);
   }

   @Override
   protected Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) throws Throwable {
      if (ctx.isInTxScope())
         return invokeNext(ctx, command);

      return invokeNextAndFinally(ctx, command, unlockAllReturnHandler);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         if (t instanceof OutdatedTopologyException)
            throw t;

         releaseLockOnTxCompletion(((TxInvocationContext) rCtx));
      });
   }

   /**
    * The backup (non-primary) owners keep a "backup lock" for each key they received in a lock/prepare command.
    * Normally there can be many transactions holding the backup lock at the same time, but when the secondary owner
    * becomes a primary owner a new transaction trying to obtain the "real" lock will have to wait for all backup
    * locks to be released. The backup lock will be released either by a commit/rollback/unlock command or by
    * the originator leaving the cluster (if recovery is disabled).
    *
    * @return {@code true} if the key was really locked.
    */
   protected final boolean lockOrRegisterBackupLock(TxInvocationContext<?> ctx, Object key, long lockTimeout)
         throws InterruptedException {
      switch (cdl.getCacheTopology().getDistribution(key).writeOwnership()) {
         case PRIMARY:
            if (trace) {
               getLog().tracef("Acquiring locks on %s.", toStr(key));
            }
            checkPendingAndLockKey(ctx, key, lockTimeout);
            return true;
         case BACKUP:
            if (trace) {
               getLog().tracef("Acquiring backup locks on %s.", key);
            }
            ctx.getCacheTransaction().addBackupLockForKey(key);
            return false;
         default:
            return false;
      }
   }

   /**
    * Same as {@link #lockOrRegisterBackupLock(TxInvocationContext, Object, long)}
    *
    * @return a collection with the keys locked.
    */
   protected final Collection<Object> lockAllOrRegisterBackupLock(TxInvocationContext<?> ctx, Collection<?> keys,
                                                                  long lockTimeout) throws InterruptedException {
      if (keys.isEmpty()) {
         return Collections.emptyList();
      }

      final Log log = getLog();
      Collection<Object> keysToLock = new ArrayList<>(keys.size());

      LocalizedCacheTopology cacheTopology = cdl.getCacheTopology();
      for (Object key : keys) {
         switch (cacheTopology.getDistribution(key).writeOwnership()) {
            case PRIMARY:
               if (trace) {
                  log.tracef("Acquiring locks on %s.", toStr(key));
               }
               keysToLock.add(key);
               break;
            case BACKUP:
               if (trace) {
                  log.tracef("Acquiring backup locks on %s.", toStr(key));
               }
               ctx.getCacheTransaction().addBackupLockForKey(key);
               break;
            default:
               break;
         }
      }

      if (keysToLock.isEmpty()) {
         return Collections.emptyList();
      }

      checkPendingAndLockAllKeys(ctx, keysToLock, lockTimeout);
      return keysToLock;
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
   private void checkPendingAndLockKey(InvocationContext ctx, Object key, long lockTimeout) throws InterruptedException {
      final long remaining = pendingLockManager.awaitPendingTransactionsForKey((TxInvocationContext<?>) ctx, key,
                                                                               lockTimeout, TimeUnit.MILLISECONDS);
      lockAndRecord(ctx, key, remaining);
   }

   private void checkPendingAndLockAllKeys(InvocationContext ctx, Collection<Object> keys, long lockTimeout)
         throws InterruptedException {
      final long remaining = pendingLockManager.awaitPendingTransactionsForAllKeys((TxInvocationContext<?>) ctx, keys,
                                                                                   lockTimeout, TimeUnit.MILLISECONDS);
      lockAllAndRecord(ctx, keys, remaining);
   }

   protected void releaseLockOnTxCompletion(TxInvocationContext ctx) {
      boolean shouldReleaseLocks = ctx.isOriginLocal() &&
            !partitionHandlingManager.isTransactionPartiallyCommitted(ctx.getGlobalTransaction());
      if (shouldReleaseLocks) {
         lockManager.unlockAll(ctx);
      }
   }
}
