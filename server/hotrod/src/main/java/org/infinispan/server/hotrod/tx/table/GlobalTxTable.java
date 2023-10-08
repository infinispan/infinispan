package org.infinispan.server.hotrod.tx.table;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;

import org.infinispan.Cache;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.table.functions.ConditionalMarkAsRollbackFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetCompletedTransactionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetDecisionFunction;
import org.infinispan.server.hotrod.tx.table.functions.TxFunction;
import org.infinispan.server.hotrod.tx.table.functions.XidPredicate;
import org.infinispan.stream.CacheCollectors;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * It is a transaction log that registers all the transaction decisions before changing the cache.
 * <p>
 * The transaction state is stored in {@link TxState}, and that is stored in a replicated cache. The {@link TxState}
 * must be updated before performing any action in the transaction (prepare, commit, etc.).
 * <p>
 * In addition, since we don't have a client crash notification, it performs a reaper work, periodically, that cleanups
 * idle transactions. The transaction is considered idle based on the timeout sent by the client. If no decision is made,
 * it rollbacks the transaction. If the transaction is completed (committed or rolled-back), it is removed from the
 * cache. If the transaction is decided (i.e. marked for commit or rollback), it completes the transaction.
 * <p>
 * Note that, recoverable transactions (transactions originated from FULL_XA support caches) aren't touched by the
 * reaper. The recovery process is responsible to handle them.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@Scope(value = Scopes.GLOBAL)
public class GlobalTxTable implements Runnable, Lifecycle {

   //TODO think about the possibility of using JGroups RAFT instead of replicated cache?

   private static final Log log = LogFactory.getLog(GlobalTxTable.class, Log.class);

   private final Cache<CacheXid, TxState> storage;
   private final FunctionalMap.ReadWriteMap<CacheXid, TxState> rwMap;
   private final GlobalComponentRegistry gcr;
   @GuardedBy("this")
   private ScheduledFuture<?> scheduledFuture;

   @Inject TimeService timeService;
   @Inject BlockingManager blockingManager;
   @Inject @ComponentName(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR)
   ScheduledExecutorService scheduledExecutor;

   public GlobalTxTable(Cache<CacheXid, TxState> storage, GlobalComponentRegistry gcr) {
      this.storage = storage;
      this.rwMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(storage.getAdvancedCache()));
      this.gcr = gcr;
   }

   @Start
   public synchronized void start() {
      //TODO where to configure it?
      //TODO can we avoid to start it here? and only when HT tx is used?
      if (scheduledFuture == null) {
         scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(this, 60000, 60000, TimeUnit.MILLISECONDS);
      }
   }

   @Stop
   public synchronized void stop() {
      if (scheduledFuture != null) {
         scheduledFuture.cancel(true);
         scheduledFuture = null;
      }
   }

   public Status update(CacheXid key, TxFunction function, long timeoutMillis) {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Updating with function: %s", key, function);
      }
      try {
         CompletableFuture<Byte> cf = rwMap.eval(key, function);
         Status status = Status.valueOf(cf.get(timeoutMillis, TimeUnit.MILLISECONDS));
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Return value is %s", key, status);
         }
         return status;
      } catch (InterruptedException e) {
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Interrupted!", key);
         }
         Thread.currentThread().interrupt();
         return Status.ERROR;
      } catch (ExecutionException | TimeoutException e) {
         if (log.isTraceEnabled()) {
            log.tracef(e, "[%s] Error!", key);
         }
         return Status.ERROR;
      }
   }

   public void markToCommit(XidImpl xid, CacheNameCollector collector) {
      markTx(xid, true, collector);
   }

   public void markToRollback(XidImpl xid, CacheNameCollector collector) {
      markTx(xid, false, collector);
   }

   public TxState getState(CacheXid xid) {
      TxState state = storage.get(xid);
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Get TxState = %s", xid, state);
      }
      return state;
   }

   public void remove(CacheXid cacheXid) {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Removed!", cacheXid);
      }
      storage.remove(cacheXid);
   }

   public void forgetTransaction(XidImpl xid) {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Forgetting transaction.", xid);
      }
      storage.keySet().parallelStream()
            .filter(new XidPredicate(xid))
            .forEach(BasicCache::remove);
   }

   /**
    * periodically checks for idle transactions and rollbacks them.
    */
   @Override
   public void run() {
      long currentTimestamp = timeService.time();
      for (Map.Entry<CacheXid, TxState> entry : storage.entrySet()) {
         TxState state = entry.getValue();
         CacheXid cacheXid = entry.getKey();
         if (!state.hasTimedOut(currentTimestamp) || skipReaper(state.getOriginator(), cacheXid.cacheName())) {
            continue;
         }
         switch (state.getStatus()) {
            case ACTIVE:
            case PREPARING:
            case PREPARED:
               onOngoingTransaction(cacheXid, state);
               break;
            case MARK_ROLLBACK:
               onTransactionDecision(cacheXid, state, false);
               break;
            case MARK_COMMIT:
               onTransactionDecision(cacheXid, state, true);
               break;
            case COMMITTED:
            case ROLLED_BACK:
               onTransactionCompleted(cacheXid);
               break;
            case ERROR:
            case NO_TRANSACTION:
            case OK:
            default:
               //not valid status
         }
      }
   }

   public Collection<XidImpl> getPreparedTransactions() {
      long currentTimestamp = timeService.time();
      Collection<XidImpl> preparedTx = new HashSet<>(); //remove duplicates!
      for (Map.Entry<CacheXid, TxState> entry : storage.entrySet()) {
         XidImpl xid = entry.getKey().xid();
         TxState state = entry.getValue();
         if (log.isTraceEnabled()) {
            log.tracef("Checking transaction xid=%s for recovery. TimedOut?=%s, Recoverable?=%s, Status=%s",
                  xid, state.hasTimedOut(currentTimestamp), state.isRecoverable(), state.getStatus());
         }
         if (state.hasTimedOut(currentTimestamp) && state.isRecoverable() && state.getStatus() == Status.PREPARED) {
            preparedTx.add(xid);
         }
      }
      return preparedTx;
   }

   public boolean isEmpty() {
      return storage.isEmpty();
   }

   private void onOngoingTransaction(CacheXid cacheXid, TxState state) {
      if (state.getStatus() == Status.PREPARED && state.isRecoverable()) {
         return; //recovery will handle prepared transactions
      }
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheXid.cacheName());
      if (cr == null) {
         //we don't have the cache locally
         return;
      }
      RpcManager rpcManager = cr.getComponent(RpcManager.class);
      if (isRemote(rpcManager, state.getOriginator())) {
         //remotely originated transaction
         //this is a weird state. the originator may crashed or it may be in another partition and communication with the client
         //in any case, we can rollback the tx
         //if we are in the minority partition, updating the global tx table will fail and we do nothing
         //if we are in the majority partition, the originator can't commit/rollback since it would fail to update the global tx table
         rollbackOldTransaction(cacheXid, state, () -> rollbackRemote(cr, cacheXid, state));
      } else {
         //local transaction prepared.
         PerCacheTxTable txTable = cr.getComponent(PerCacheTxTable.class);
         EmbeddedTransaction tx = txTable.getLocalTx(cacheXid.xid());
         if (tx == null) {
            //local transaction doesn't exists.
            onTransactionCompleted(cacheXid);
         } else {
            blockingManager.runBlocking(
                  () -> rollbackOldTransaction(cacheXid, state, () -> completeLocal(txTable, cacheXid, tx, false)),
                  cacheXid);
         }
      }
   }

   private void rollbackOldTransaction(CacheXid cacheXid, TxState state, Runnable onSuccessAction) {
      TxFunction txFunction = new ConditionalMarkAsRollbackFunction(state.getStatus());
      rwMap.eval(cacheXid, txFunction).thenAccept(aByte -> {
         if (aByte == Status.OK.value) {
            onSuccessAction.run();
         }
      });
   }

   private void rollbackRemote(ComponentRegistry cr, CacheXid cacheXid, TxState state) {
      RollbackCommand rpcCommand = cr.getCommandsFactory().buildRollbackCommand(state.getGlobalTransaction());
      RpcManager rpcManager = cr.getComponent(RpcManager.class);
      rpcCommand.setTopologyId(rpcManager.getTopologyId());
      rpcManager.invokeCommandOnAll(rpcCommand, VoidResponseCollector.validOnly(), rpcManager.getSyncRpcOptions())
            .thenRun(() -> {
               //ignore exception so the rollback can be retried.
               //if a node doesn't find the remote transaction, it returns null.
               TxFunction function = new SetCompletedTransactionFunction(false);
               rwMap.eval(cacheXid, function);
            });
   }

   private void onTransactionDecision(CacheXid cacheXid, TxState state, boolean commit) {
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheXid.cacheName());
      if (cr == null) {
         //we don't have the cache locally
         return;
      }
      RpcManager rpcManager = cr.getComponent(RpcManager.class);
      if (rpcManager == null || state.getOriginator().equals(rpcManager.getAddress())) {
         //local
         PerCacheTxTable txTable = cr.getComponent(PerCacheTxTable.class);
         EmbeddedTransaction tx = txTable.getLocalTx(cacheXid.xid());
         if (tx == null) {
            //transaction completed
            onTransactionCompleted(cacheXid);
         } else {
            blockingManager.runBlocking(() -> completeLocal(txTable, cacheXid, tx, commit), cacheXid);
         }
      } else {
         if (commit) {
            TransactionBoundaryCommand rpcCommand;
            if (cr.getComponent(Configuration.class).transaction().lockingMode() == LockingMode.PESSIMISTIC) {
               rpcCommand = cr.getCommandsFactory()
                     .buildPrepareCommand(state.getGlobalTransaction(), state.getModifications(), true);
            } else {
               rpcCommand = cr.getCommandsFactory().buildCommitCommand(state.getGlobalTransaction());
            }
            rpcCommand.setTopologyId(rpcManager.getTopologyId());
            rpcManager.invokeCommandOnAll(rpcCommand, VoidResponseCollector.validOnly(), rpcManager.getSyncRpcOptions())
                  .handle((aVoid, throwable) -> {
                     //TODO?
                     TxFunction function = new SetCompletedTransactionFunction(true);
                     rwMap.eval(cacheXid, function);
                     return null;
                  });
         } else {
            rollbackRemote(cr, cacheXid, state);
         }
      }
   }

   private void completeLocal(PerCacheTxTable txTable, CacheXid cacheXid, EmbeddedTransaction tx,
         boolean commit) {
      try {
         tx.runCommit(!commit);
      } catch (HeuristicMixedException | HeuristicRollbackException | RollbackException e) {
         //embedded tx cleanups everything
         //TODO log
      } finally {
         txTable.removeLocalTx(cacheXid.xid());
      }
      onTransactionCompleted(cacheXid);
   }

   private void onTransactionCompleted(CacheXid cacheXid) {
      storage.removeAsync(cacheXid);
   }

   private boolean skipReaper(Address originator, ByteString cacheName) {
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
      if (cr == null) {
         //cache is stopped? doesn't exist? we need to handle it
         return false;
      }
      RpcManager rpcManager = cr.getComponent(RpcManager.class);
      return isRemote(rpcManager, originator) && //we are not the originator. I
             rpcManager.getMembers().contains(originator); //originator is still in the view.
   }

   private boolean isRemote(RpcManager rpcManager, Address originator) {
      return rpcManager != null && !originator.equals(rpcManager.getAddress());
   }

   private List<CacheXid> getKeys(XidImpl xid) {
      return storage.keySet().stream()
            .filter(new XidPredicate(xid))
            .collect(CacheCollectors.serializableCollector(Collectors::toList));
   }

   private void markTx(XidImpl xid, boolean commit, CacheNameCollector collector) {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Set Transaction Decision to %s", xid, commit ? "Commit" : "Rollback");
      }
      final List<CacheXid> cacheXids = getKeys(xid);
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Fetched CacheXids=%s", xid, cacheXids);
      }
      final int size = cacheXids.size();
      if (size == 0) {
         collector.noTransactionFound();
         return;
      }
      collector.expectedSize(size);

      SetDecisionFunction function = new SetDecisionFunction(commit);
      for (CacheXid cacheXid : cacheXids) {
         rwMap.eval(cacheXid, function).handle((statusValue, throwable) -> {
            Status status;
            if (throwable == null) {
               status = Status.valueOf(statusValue);
            } else {
               status = Status.ERROR;
            }
            collector.addCache(cacheXid.cacheName(), status);
            return null;
         });
      }
   }
}
