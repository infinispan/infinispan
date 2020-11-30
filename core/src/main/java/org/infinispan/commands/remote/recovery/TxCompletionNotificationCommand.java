package org.infinispan.commands.remote.recovery;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Command for removing recovery related information from the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class TxCompletionNotificationCommand extends BaseRpcCommand implements TopologyAffectedCommand {
   private static final Log log = LogFactory.getLog(TxCompletionNotificationCommand.class);

   public static final int COMMAND_ID = 22;

   private XidImpl xid;
   private long internalId;
   private GlobalTransaction gtx;
   private int topologyId = -1;

   private TxCompletionNotificationCommand() {
      super(null); // For command id uniqueness test
   }

   public TxCompletionNotificationCommand(XidImpl xid, GlobalTransaction gtx, ByteString cacheName) {
      super(cacheName);
      this.xid = xid;
      this.gtx = gtx;
   }

   public TxCompletionNotificationCommand(long internalId, ByteString cacheName) {
      super(cacheName);
      this.internalId = internalId;
   }

   public TxCompletionNotificationCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      if (log.isTraceEnabled())
         log.tracef("Processing completed transaction %s", gtx);
      RemoteTransaction remoteTx = null;
      RecoveryManager recoveryManager = componentRegistry.getRecoveryManager().running();
      if (recoveryManager != null) { //recovery in use
         if (xid != null) {
            remoteTx = (RemoteTransaction) recoveryManager.removeRecoveryInformation(xid);
         } else {
            remoteTx = (RemoteTransaction) recoveryManager.removeRecoveryInformation(internalId);
         }
      }
      if (remoteTx == null && gtx != null) {
         TransactionTable txTable = componentRegistry.getTransactionTableRef().running();
         remoteTx = txTable.removeRemoteTransaction(gtx);
      }
      if (remoteTx == null) return CompletableFutures.completedNull();
      forwardCommandRemotely(componentRegistry.getStateTransferManager(), remoteTx);

      LockManager lockManager = componentRegistry.getLockManager().running();
      lockManager.unlockAll(remoteTx.getLockedKeys(), remoteTx.getGlobalTransaction());
      return CompletableFutures.completedNull();
   }

   public GlobalTransaction getGlobalTransaction() {
      return gtx;
   }

   /**
    * This only happens during state transfer.
    */
   private void forwardCommandRemotely(StateTransferManager stateTransferManager, RemoteTransaction remoteTx) {
      Set<Object> affectedKeys = remoteTx.getAffectedKeys();
      if (log.isTraceEnabled())
         log.tracef("Invoking forward of TxCompletionNotification for transaction %s. Affected keys: %s", gtx,
               toStr(affectedKeys));
      stateTransferManager.forwardCommandIfNeeded(this, affectedKeys, remoteTx.getGlobalTransaction().getAddress());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      if (xid == null) {
         output.writeBoolean(true);
         output.writeLong(internalId);
      } else {
         output.writeBoolean(false);
         XidImpl.writeTo(output, xid);
      }
      output.writeObject(gtx);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      if (input.readBoolean()) {
         internalId = input.readLong();
      } else {
         xid = XidImpl.readFrom(input);
      }
      gtx = (GlobalTransaction) input.readObject();
   }

   @Override
   public boolean canBlock() {
      //this command can be forwarded (state transfer)
      return true;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{ xid=" + xid +
            ", internalId=" + internalId +
            ", topologyId=" + topologyId +
            ", gtx=" + gtx +
            ", cacheName=" + cacheName + "} ";
   }
}
