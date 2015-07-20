package org.infinispan.interceptors.locking;

import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.concurrent.locks.LockUtil;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.logging.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Base class for transaction based locking interceptors.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.1
 */
public abstract class AbstractTxLockingInterceptor extends AbstractLockingInterceptor {

   protected RpcManager rpcManager;
   private PartitionHandlingManager partitionHandlingManager;
   private PendingLockManager pendingLockManager;

   @Inject
   public void setDependencies(RpcManager rpcManager,
                               PartitionHandlingManager partitionHandlingManager,
                               PendingLockManager pendingLockManager) {
      this.rpcManager = rpcManager;
      this.partitionHandlingManager = partitionHandlingManager;
      this.pendingLockManager = pendingLockManager;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
         // Cache.putForExternalRead() is non-transactional
         return visitNonTxDataWriteCommand(ctx, command);
      }
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      try {
         return super.visitGetAllCommand(ctx, command);
      } finally {
         //when not invoked in an explicit tx's scope the get is non-transactional(mainly for efficiency).
         //locks need to be released in this situation as they might have been acquired from L1.
         if (!ctx.isInTxScope()) lockManager.unlockAll(ctx);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      boolean releaseLocks = releaseLockOnTxCompletion(ctx);
      try {
         return super.visitCommitCommand(ctx, command);
      } catch (OutdatedTopologyException e) {
         releaseLocks = false;
         throw e;
      } finally {
         if (releaseLocks) lockManager.unlockAll(ctx);
      }
   }

   protected final Object invokeNextAndCommitIf1Pc(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object result = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit() && releaseLockOnTxCompletion(ctx)) {
         lockManager.unlockAll(ctx);
      }
      return result;
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
      final Log log = getLog();
      final boolean trace = log.isTraceEnabled();

      switch (LockUtil.getLockOwnership(key, cdl)) {
         case PRIMARY:
            if (trace) {
               log.tracef("Acquiring locks on %s.", key);
            }
            checkPendingAndLockKey(ctx, key, lockTimeout);
            return true;
         case BACKUP:
            if (trace) {
               log.tracef("Acquiring backup locks on %s.", key);
            }
            ctx.getCacheTransaction().addBackupLockForKey(key);
         default:
            return false;
      }
   }

   /**
    * Same as {@link #lockOrRegisterBackupLock(TxInvocationContext, Object, long)}
    *
    * @return a collection with the keys locked.
    */
   protected final Collection<Object> lockAllOrRegisterBackupLock(TxInvocationContext<?> ctx, Collection<Object> keys,
                                                                  long lockTimeout) throws InterruptedException {
      if (keys.isEmpty()) {
         return Collections.emptyList();
      }

      final Log log = getLog();
      final boolean trace = log.isTraceEnabled();

      Collection<Object> keysToLock = new ArrayList<>(keys.size());

      for (Object key : keys) {
         switch (LockUtil.getLockOwnership(key, cdl)) {
            case PRIMARY:
               if (trace) {
                  log.tracef("Acquiring locks on %s.", key);
               }
               keysToLock.add(key);
               break;
            case BACKUP:
               if (trace) {
                  log.tracef("Acquiring backup locks on %s.", key);
               }
               ctx.getCacheTransaction().addBackupLockForKey(key);
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

   private boolean releaseLockOnTxCompletion(TxInvocationContext ctx) {
      return (ctx.isOriginLocal() && !partitionHandlingManager.isTransactionPartiallyCommitted(ctx.getGlobalTransaction()) ||
                    (!ctx.isOriginLocal() && Configurations.isSecondPhaseAsync(cacheConfiguration)));
   }
}
