package org.infinispan.transaction;

import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.XAException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * Transaction coordinator to be used when the cache is clustered.
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
public class ClusteredTransactionCoordinator extends TransactionCoordinator {

   private static Log log = LogFactory.getLog(ClusteredTransactionCoordinator.class);

   private ExecutorService asyncExecutor;
   private ClusteringDependentLogic clusteringLogic;
   private RpcManager rpcManager;
   private RecoveryManager recoveryManager;
   private boolean isRecoveryActive;

   @Inject
   public void init(@ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncExecutor,
                    ClusteringDependentLogic clusteringDependentLogic, RpcManager rpcManager, RecoveryManager recoveryManager) {
      this.asyncExecutor = asyncExecutor;
      this.clusteringLogic = clusteringDependentLogic;
      this.rpcManager = rpcManager;
      this.recoveryManager = recoveryManager;
   }

   @Start
   public void start() {
      super.start();
      this.isRecoveryActive = configuration.isTransactionalCache() && configuration.isTransactionRecoveryEnabled();
   }

   protected final void runSecondPhase(final LocalTransaction localTransaction, final LocalTxInvocationContext ctx) throws XAException {
      if (!configuration.isSyncCommitPhase() && !localTransaction.isTopologyChanged(clusteringLogic)) {
         asyncExecutor.execute(new Runnable() {
            @Override
            public void run() {
               try {
                  log.tracef("Invoking commit asynchronously for transaction %s", localTransaction);
                  runCommit(localTransaction, ctx);
               } catch (XAException e) {
                  log.trace("Commit failed", e);
               }
               log.trace("Commit finished, releasing locks...");
               //do it sync, otherwise it will be processed by another thread from the same thread pool..
               releaseLocksIfNeeded(false, localTransaction);
               log.tracef("Locks released for transaction %s", localTransaction);
            }
         });
      } else {
         runCommit(localTransaction, ctx);
         releaseLocksIfNeeded(false, localTransaction);
      }
   }

   protected final void releaseLocksIfNeeded(boolean sync, LocalTransaction localTransaction) {
      if (!isRecoveryActive && localTransaction.mightHaveRemoteLocks() && !configuration.isSecondPhaseAsync()) {
         final TxCompletionNotificationCommand command =
               commandsFactory.buildTxCompletionNotificationCommand(null, localTransaction.getGlobalTransaction());
         final Collection<Address> owners = clusteringLogic.getOwners(localTransaction.getAffectedKeys());
         log.tracef("About to invoke tx completion notification on nodes %s", owners);
         rpcManager.invokeRemotely(owners, command, sync, true);
      } else if (isRecoveryActive) {
         LocalXaTransaction localXaTransaction = (LocalXaTransaction) localTransaction;
         recoveryManager.removeRecoveryInformationFromCluster(localTransaction.getRemoteLocksAcquired(), localXaTransaction.getXid(),
                                                              false, localTransaction.getGlobalTransaction());
         txTable.removeLocalTransaction(localTransaction);

      }
   }

   @Override
   protected final void completeTransactionInOnePhase(LocalTransaction localTransaction, LocalTxInvocationContext ctx) throws XAException {
      super.completeTransactionInOnePhase(localTransaction, ctx);
      releaseLocksIfNeeded(false, localTransaction);
   }

   public void setRecoveryManager(RecoveryManager recoveryManager) {
      this.recoveryManager = recoveryManager;
   }
}
