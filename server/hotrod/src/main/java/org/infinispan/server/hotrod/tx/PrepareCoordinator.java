package org.infinispan.server.hotrod.tx;

import static org.infinispan.remoting.transport.impl.VoidResponseCollector.validOnly;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.PerCacheTxTable;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.server.hotrod.tx.table.functions.CreateStateFunction;
import org.infinispan.server.hotrod.tx.table.functions.PreparingDecisionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetCompletedTransactionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetDecisionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetPreparedFunction;
import org.infinispan.server.hotrod.tx.table.functions.TxFunction;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.ByteString;

/**
 * A class that handles the prepare request from the Hot Rod clients.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class PrepareCoordinator {

   private static final Log log = Log.getLog(MethodHandles.lookup().lookupClass());

   private final AdvancedCache<?, ?> cache;
   private final XidImpl xid;
   private final PerCacheTxTable perCacheTxTable;
   private final TransactionTable transactionTable;
   private final CacheXid cacheXid;
   private final GlobalTxTable globalTxTable;
   private final long transactionTimeout;
   private EmbeddedTransaction tx;
   private final boolean recoverable;

   public PrepareCoordinator(AdvancedCache<byte[], byte[]> cache, XidImpl xid, boolean recoverable,
         long transactionTimeout) {
      this.xid = xid;
      this.recoverable = recoverable;
      this.transactionTimeout = transactionTimeout;
      this.cache = cache;
      ComponentRegistry registry = ComponentRegistry.of(cache);
      transactionTable = registry.getComponent(TransactionTable.class);
      perCacheTxTable = registry.getComponent(PerCacheTxTable.class);
      globalTxTable = registry.getGlobalComponentRegistry().getComponent(GlobalTxTable.class);
      cacheXid = new CacheXid(ByteString.fromString(cache.getName()), xid);
   }

   /**
    * @return The current {@link TxState} associated to the transaction.
    */
   public final TxState getTxState() {
      return globalTxTable.getState(cacheXid);
   }

   /**
    * @return {@code true} if the {@code address} is still in the cluster.
    */
   public final boolean isAlive(Address address) {
      RpcManager rpcManager = cache.getRpcManager();
      return rpcManager == null || rpcManager.getMembers().contains(address);
   }

   /**
    * Rollbacks a transaction that is remove in all the cluster members.
    */
   public final void rollbackRemoteTransaction(GlobalTransaction gtx) {
      RpcManager rpcManager = cache.getRpcManager();
      ComponentRegistry componentRegistry = ComponentRegistry.of(cache);
      CommandsFactory factory = componentRegistry.getCommandsFactory();
      try {
         RollbackCommand rollbackCommand = factory.buildRollbackCommand(gtx);
         rollbackCommand.setTopologyId(rpcManager.getTopologyId());
         CompletionStage<Void> cs = rpcManager
               .invokeCommandOnAll(rollbackCommand, validOnly(), rpcManager.getSyncRpcOptions());
         rollbackCommand.invokeAsync(componentRegistry).toCompletableFuture().join();
         cs.toCompletableFuture().join();
      } catch (Throwable throwable) {
         throw Util.rewrapAsCacheException(CompletableFutures.extractException(throwable));
      } finally {
         forgetTransaction(gtx, rpcManager, factory);
      }
   }

   /**
    * Starts a transaction.
    *
    * @return {@code true} if the transaction can be started, {@code false} otherwise.
    */
   public boolean startTransaction() {
      EmbeddedTransaction tx = beginTransaction();
      if (tx == null) {
         return false;
      }
      tx.setXid(xid);
      LocalTransaction localTransaction = transactionTable
            .getOrCreateLocalTransaction(tx, false, this::newGlobalTransaction);
      var gtx = localTransaction.getGlobalTransaction();
      if (!cache.getCacheConfiguration().transaction().useSynchronization()) {
         gtx.setXid(xid);
      }
      if (createGlobalState(gtx) != Status.OK) {
         //no need to rollback. nothing is enlisted in the transaction.
         transactionTable.removeLocalTransaction(localTransaction);
         EmbeddedTransactionManager.dissociateTransaction();
         return false;
      } else {
         this.tx = tx;
         perCacheTxTable.createLocalTx(xid, tx);
         transactionTable.enlistClientTransaction(tx, localTransaction);
         return true;
      }
   }

   /**
    * Rollbacks the transaction if an exception happens during the transaction execution.
    */
   public int rollback() {
      //log rolling back
      loggingDecision(false);

      //perform rollback
      try {
         tx.rollback();
      } catch (SystemException e) {
         //ignore exception (heuristic exceptions)
         log.debug("Ignoring exception while rolling back transaction", e);
      } finally {
         perCacheTxTable.removeLocalTx(xid);
      }

      //log rolled back
      loggingCompleted(false);
      return XAException.XA_RBROLLBACK;
   }

   /**
    * Marks the transaction as rollback-only.
    */
   public void setRollbackOnly() {
      tx.setRollbackOnly();
   }

   /**
    * Prepares the transaction.
    *
    * @param onePhaseCommit {@code true} if one phase commit.
    * @return the {@link javax.transaction.xa.XAResource#XA_OK} if successful prepared, otherwise one of the {@link
    * javax.transaction.xa.XAException} error codes.
    */
   public int prepare(boolean onePhaseCommit) {
      Status status = loggingPreparing();
      if (status != Status.OK) {
         //error, marked_*, no_transaction code (other node changed the state). we simply reply with rollback
         return XAException.XA_RBROLLBACK;
      }
      boolean prepared = tx.runPrepare();
      if (prepared) {
         if (onePhaseCommit) {
            return onePhaseCommitTransaction();
         } else {
            status = loggingPrepared();
            return status == Status.OK ? XAResource.XA_OK : XAException.XA_RBROLLBACK;
         }
      } else {
         //Infinispan automatically rollbacks the transaction
         //we try to update the state and we don't care about the response.
         loggingCompleted(false);
         perCacheTxTable.removeLocalTx(xid);
         return XAException.XA_RBROLLBACK;
      }
   }

   /**
    * Commits a remote 1PC transaction that is already in MARK_COMMIT state
    */
   public int onePhaseCommitRemoteTransaction(GlobalTransaction gtx, List<WriteCommand> modifications) {
      RpcManager rpcManager = cache.getRpcManager();
      ComponentRegistry componentRegistry = ComponentRegistry.of(cache);
      CommandsFactory factory = componentRegistry.getCommandsFactory();
      try {
         //only pessimistic tx are committed in 1PC and it doesn't use versions.
         PrepareCommand command = factory.buildPrepareCommand(gtx, modifications, true);
         CompletionStage<Void> cs = rpcManager.invokeCommandOnAll(command, validOnly(), rpcManager.getSyncRpcOptions());
         command.invokeAsync(componentRegistry).toCompletableFuture().join();
         cs.toCompletableFuture().join();
         forgetTransaction(gtx, rpcManager, factory);
         return loggingCompleted(true) == Status.OK ?
                XAResource.XA_OK :
                XAException.XAER_RMERR;
      } catch (Throwable throwable) {
         //transaction should commit but we still can have exceptions (timeouts or similar)
         return XAException.XAER_RMERR;
      }
   }

   /**
    * Forgets the transaction cluster-wise and from global and local transaction tables.
    */
   private void forgetTransaction(GlobalTransaction gtx, RpcManager rpcManager, CommandsFactory factory) {
      TxCompletionNotificationCommand cmd = factory.buildTxCompletionNotificationCommand(xid, gtx);
      rpcManager.sendToAll(cmd, DeliverOrder.NONE);
      perCacheTxTable.removeLocalTx(xid);
      globalTxTable.remove(cacheXid);
   }

   private Status loggingDecision(boolean commit) {
      TxFunction function = new SetDecisionFunction(commit);
      return globalTxTable.update(cacheXid, function, transactionTimeout);
   }

   private Status loggingCompleted(boolean committed) {
      TxFunction function = new SetCompletedTransactionFunction(committed);
      return globalTxTable.update(cacheXid, function, transactionTimeout);
   }

   private int onePhaseCommitTransaction() {
      if (loggingDecision(true) != Status.OK) {
         //we failed to update the global cache
         return XAException.XAER_RMERR;
      }
      try {
         tx.runCommit(false);
         return loggingCompleted(true) == Status.OK ?
                XAResource.XA_OK :
                XAException.XAER_RMERR; //we failed to update the global cache.
      } catch (HeuristicMixedException | HeuristicRollbackException | RollbackException e) {
         //Infinispan automatically rollbacks it
         loggingCompleted(false);
         return XAException.XA_RBROLLBACK;
      }
   }

   private Status loggingPrepared() {
      SetPreparedFunction function = new SetPreparedFunction();
      return globalTxTable.update(cacheXid, function, transactionTimeout);
   }

   private Status createGlobalState(GlobalTransaction globalTransaction) {
      CreateStateFunction function = new CreateStateFunction(globalTransaction, recoverable, transactionTimeout);
      return globalTxTable.update(cacheXid, function, transactionTimeout);
   }

   private Status loggingPreparing() {
      TxFunction function = new PreparingDecisionFunction(copyModifications());
      return globalTxTable.update(cacheXid, function, transactionTimeout);
   }


   private List<WriteCommand> copyModifications() {
      List<WriteCommand> modifications = getLocalTransaction().getModifications();
      return new ArrayList<>(modifications);
   }

   private LocalTransaction getLocalTransaction() {
      return transactionTable.getLocalTransaction(tx);
   }

   private GlobalTransaction newGlobalTransaction() {
      TransactionFactory factory =  ComponentRegistry.componentOf(cache, TransactionFactory.class);
      return factory.newGlobalTransaction(cache.getCacheManager().getAddress(), false, true);
   }

   private EmbeddedTransaction beginTransaction() {
      try {
         EmbeddedTransactionManager tm = (EmbeddedTransactionManager) cache.getTransactionManager();
         tm.begin();
         return tm.getTransaction();
      } catch (Exception e) {
         // EmbeddedTransactionManager throws NotSupportedException if a transaction is already running.
         // It should not happen.
         throw new IllegalStateException("Unable to begin transaction. Please report this exception. Current transaction: %s, client xid=%s".formatted(EmbeddedTransactionManager.getCurrentTransaction(), cacheXid), e);
      }
   }
}
