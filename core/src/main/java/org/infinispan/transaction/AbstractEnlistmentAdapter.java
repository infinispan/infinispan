package org.infinispan.transaction;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.DeltaCompositeKeyUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

import static org.infinispan.util.DeltaCompositeKeyUtil.filterDeltaCompositeKeys;

/**
 * Base class for both Sync and XAResource enlistment adapters.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractEnlistmentAdapter {

   private static Log log = LogFactory.getLog(AbstractEnlistmentAdapter.class);

   private final CommandsFactory commandsFactory;
   private final RpcManager rpcManager;
   private final TransactionTable txTable;
   private final ClusteringDependentLogic clusteringLogic;
   private final int hashCode;
   private final boolean isSecondPhaseAsync;
   private final boolean isPessimisticLocking;
   private final boolean isTotalOrder;
   protected final TransactionCoordinator txCoordinator;

   public AbstractEnlistmentAdapter(CacheTransaction cacheTransaction,
            CommandsFactory commandsFactory, RpcManager rpcManager,
            TransactionTable txTable, ClusteringDependentLogic clusteringLogic,
            Configuration configuration, TransactionCoordinator txCoordinator) {
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.txTable = txTable;
      this.clusteringLogic = clusteringLogic;
      this.isSecondPhaseAsync = Configurations.isSecondPhaseAsync(configuration);
      this.isPessimisticLocking = configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      this.isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
      hashCode = preComputeHashCode(cacheTransaction);
      this.txCoordinator = txCoordinator;
   }

   public AbstractEnlistmentAdapter(CommandsFactory commandsFactory,
            RpcManager rpcManager, TransactionTable txTable,
            ClusteringDependentLogic clusteringLogic, Configuration configuration, TransactionCoordinator txCoordinator) {
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.txTable = txTable;
      this.clusteringLogic = clusteringLogic;
      this.isSecondPhaseAsync = Configurations.isSecondPhaseAsync(configuration);
      this.isPessimisticLocking = configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      this.isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
      hashCode = 31;
      this.txCoordinator = txCoordinator;
   }

   protected final void releaseLocksForCompletedTransaction(LocalTransaction localTransaction, boolean committedInOnePhase) {
      final GlobalTransaction gtx = localTransaction.getGlobalTransaction();
      txTable.removeLocalTransaction(localTransaction);
      log.tracef("Committed in onePhase? %s isOptimistic? %s", committedInOnePhase, isOptimisticCache());
      if (committedInOnePhase && isOptimisticCache())
         return;
      if (isClustered()) {
         removeTransactionInfoRemotely(localTransaction, gtx);
      }
   }

   private void removeTransactionInfoRemotely(LocalTransaction localTransaction, GlobalTransaction gtx) {
      if (mayHaveRemoteLocks(localTransaction) && !isSecondPhaseAsync) {
         final TxCompletionNotificationCommand command = commandsFactory.buildTxCompletionNotificationCommand(null, gtx);
         final Collection<Address> owners = clusteringLogic.getOwners(filterDeltaCompositeKeys(localTransaction.getAffectedKeys()));
         Collection<Address> commitNodes = localTransaction.getCommitNodes(owners, rpcManager.getTopologyId(), rpcManager.getMembers());
         log.tracef("About to invoke tx completion notification on commitNodes: %s", commitNodes);
         rpcManager.invokeRemotely(commitNodes, command, rpcManager.getDefaultRpcOptions(false, false));
      }
   }

   private boolean mayHaveRemoteLocks(LocalTransaction lt) {
      return !isTotalOrder && (lt.getRemoteLocksAcquired() != null && !lt.getRemoteLocksAcquired().isEmpty() ||
            !lt.getModifications().isEmpty() ||
            isPessimisticLocking && lt.getTopologyId() != rpcManager.getTopologyId());
   }

   /**
    * Invoked by TransactionManagers, make sure it's an efficient implementation.
    * System.identityHashCode(x) is NOT an efficient implementation.
    */
   @Override
   public final int hashCode() {
      return this.hashCode;
   }

   private static int preComputeHashCode(final CacheTransaction cacheTx) {
      return 31 + cacheTx.hashCode();
   }

   private boolean isClustered() {
      return rpcManager != null;
   }

   private boolean isOptimisticCache() {
      //a transactional cache that is neither total order nor pessimistic must be optimistic.
      return !isPessimisticLocking && !isTotalOrder;
   }
}
