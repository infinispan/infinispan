package org.infinispan.commands.remote.recovery;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.Xid;

import java.util.Set;

/**
 * Command for removing recovery related information from the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class TxCompletionNotificationCommand  extends RecoveryCommand implements TopologyAffectedCommand {

   private static Log log = LogFactory.getLog(TxCompletionNotificationCommand.class);

   public static final int COMMAND_ID = 22;

   private Xid xid;
   private long internalId;
   private GlobalTransaction gtx;
   private TransactionTable txTable;
   private LockManager lockManager;
   private StateTransferManager stateTransferManager;
   private int topologyId = -1;

   private TxCompletionNotificationCommand() {
      super(null); // For command id uniqueness test
   }

   public TxCompletionNotificationCommand(Xid xid, GlobalTransaction gtx, String cacheName) {
      super(cacheName);
      this.xid = xid;
      this.gtx = gtx;
   }

   public void init(TransactionTable tt, LockManager lockManager, RecoveryManager rm, StateTransferManager stm) {
      super.init(rm);
      this.txTable = tt;
      this.lockManager = lockManager;
      this.stateTransferManager = stm;
   }


   public TxCompletionNotificationCommand(long internalId, String cacheName) {
      super(cacheName);
      this.internalId = internalId;
   }

   public TxCompletionNotificationCommand(String cacheName) {
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
   public Object perform(InvocationContext ctx) throws Throwable {
      log.tracef("Processing completed transaction %s", gtx);
      RemoteTransaction remoteTx = null;
      if (recoveryManager != null) { //recovery in use
         if (xid != null) {
            remoteTx = (RemoteTransaction) recoveryManager.removeRecoveryInformation(xid);
         } else {
            remoteTx = (RemoteTransaction) recoveryManager.removeRecoveryInformation(internalId);
         }
      }
      if (remoteTx == null && gtx != null) {
         remoteTx = txTable.removeRemoteTransaction(gtx);
      }
      boolean successful = remoteTx != null && !remoteTx.isMarkedForRollback();
      if (gtx != null) {
         txTable.markTransactionCompleted(gtx, successful);
      } else if (remoteTx != null) {
         txTable.markTransactionCompleted(remoteTx.getGlobalTransaction(), successful);
      }
      if (remoteTx == null) return null;
      forwardCommandRemotely(remoteTx);

      lockManager.unlockAll(remoteTx.getLockedKeys(), remoteTx.getGlobalTransaction());
      return null;
   }

   public GlobalTransaction getGlobalTransaction() {
      return gtx;
   }

   /**
    * This only happens during state transfer.
    */
   private void forwardCommandRemotely(RemoteTransaction remoteTx) {
      Set<Object> affectedKeys = remoteTx.getAffectedKeys();
      log.tracef("Invoking forward of TxCompletionNotification for transaction %s. Affected keys: %s", gtx, affectedKeys);
      stateTransferManager.forwardCommandIfNeeded(this, affectedKeys, remoteTx.getGlobalTransaction().getAddress());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{xid != null ? xid : internalId, gtx};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Wrong command id. Received " + commandId + " and expected " + TxCompletionNotificationCommand.COMMAND_ID);
      }
      if (parameters[0] instanceof Xid) {
         xid = (Xid) parameters[0];
      } else {
         internalId = (Long) parameters[0];
      }
      gtx = (GlobalTransaction) parameters[1];
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
