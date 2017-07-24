package org.infinispan.server.hotrod.tx;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.TransactionTable;

/**
 * A base decode context to handle prepare, commit and rollback requests from a client.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
abstract class TransactionDecodeContext {

   final XidImpl xid;
   final ServerTransactionTable serverTransactionTable;
   final RpcManager rpcManager;
   final Address localAddress;
   final TransactionTable transactionTable;
   final CommandsFactory commandsFactory;
   private final ClusteringDependentLogic clusteringDependentLogic;
   TxState txState;

   TransactionDecodeContext(AdvancedCache<byte[], byte[]> cache, XidImpl xid) {
      this.xid = xid;
      this.rpcManager = cache.getRpcManager();
      this.localAddress = rpcManager != null ? rpcManager.getAddress() : null;
      ComponentRegistry registry = cache.getComponentRegistry();
      this.commandsFactory = registry.getCommandsFactory();
      this.transactionTable = registry.getComponent(TransactionTable.class);
      this.clusteringDependentLogic = registry.getComponent(ClusteringDependentLogic.class);
      this.serverTransactionTable = registry.getComponent(ServerTransactionTable.class);
      this.txState = serverTransactionTable.getGlobalState(xid);
   }

   /**
    * @return The current {@link TxState} associated to the transaction.
    */
   public final TxState getTxState() {
      return txState;
   }

   /**
    * @return {@code true} if the {@code address} is still in the cluster.
    */
   public final boolean isAlive(Address address) {
      return clusteringDependentLogic.getCacheTopology().getActualMembers().contains(address);
   }

   /**
    * Rollbacks a transaction that is remove in all the cluster members.
    */
   public final void rollbackRemoteTransaction() {
      try {
         RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(txState.getGlobalTransaction());
         rpcManager.invokeRemotely(null, rollbackCommand, rpcManager.getDefaultRpcOptions(true));
         commandsFactory.initializeReplicableCommand(rollbackCommand, false);
         rollbackCommand.invokeAsync().join();
      } catch (Throwable throwable) {
         throw Util.rewrapAsCacheException(throwable);
      } finally {
         forgetTransaction();
      }
   }

   /**
    * Advances the {@link TxState}.
    */
   final void advance(TxState update) {
      if (update == null || !serverTransactionTable.updateGlobalState(xid, txState, update)) {
         throw new IllegalStateException();
      }
      txState = update;
   }

   /**
    * Forgets the transaction cluster-wise and from global and local transaction tables.
    */
   final void forgetTransaction() {
      TxCompletionNotificationCommand cmd = commandsFactory
            .buildTxCompletionNotificationCommand(xid, txState.getGlobalTransaction());
      rpcManager.invokeRemotelyAsync(null, cmd, rpcManager.getDefaultRpcOptions(false, DeliverOrder.NONE));
      serverTransactionTable.removeGlobalState(xid);
   }

   /**
    * @return {@code true} if the cache mode is local.
    */
   final boolean isLocalMode() {
      return rpcManager == null;
   }

}
