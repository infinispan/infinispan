package org.infinispan.commands.remote.recovery;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.transaction.xa.Xid;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserAwareObjectOutput;
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
public class TxCompletionNotificationCommand  extends RecoveryCommand implements TopologyAffectedCommand {
   private static final Log log = LogFactory.getLog(TxCompletionNotificationCommand.class);
   private static final boolean trace = log.isTraceEnabled();

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

   public TxCompletionNotificationCommand(Xid xid, GlobalTransaction gtx, ByteString cacheName) {
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
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      if (trace)
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
      if (remoteTx == null) return CompletableFutures.completedNull();
      forwardCommandRemotely(remoteTx);

      lockManager.unlockAll(remoteTx.getLockedKeys(), remoteTx.getGlobalTransaction());
      return CompletableFutures.completedNull();
   }

   public GlobalTransaction getGlobalTransaction() {
      return gtx;
   }

   /**
    * This only happens during state transfer.
    */
   private void forwardCommandRemotely(RemoteTransaction remoteTx) {
      Set<Object> affectedKeys = remoteTx.getAffectedKeys();
      if (trace)
         log.tracef("Invoking forward of TxCompletionNotification for transaction %s. Affected keys: %s", gtx,
               toStr(affectedKeys));
      stateTransferManager.forwardCommandIfNeeded(this, affectedKeys, remoteTx.getGlobalTransaction().getAddress());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserAwareObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      if (xid == null) {
         output.writeBoolean(true);
         output.writeLong(internalId);
      } else {
         output.writeBoolean(false);
         output.writeObject(xid);
      }
      output.writeObject(gtx);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      if (input.readBoolean()) {
         internalId = input.readLong();
      } else {
         xid = (Xid) input.readObject();
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
