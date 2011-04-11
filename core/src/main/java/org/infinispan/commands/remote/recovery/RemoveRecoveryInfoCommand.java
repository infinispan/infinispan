package org.infinispan.commands.remote.recovery;

import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.Ids;

import javax.transaction.xa.Xid;

/**
 * Command for removing recovery related information from the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RemoveRecoveryInfoCommand extends RecoveryCommand {

   public static final int COMMAND_ID = Ids.REMOVE_RECOVERY_INFO_TX_COMMAND;

   private Xid xid;
   private long internalId;

   public RemoveRecoveryInfoCommand(Xid xid, String cacheName) {
      this.xid = xid;
      this.cacheName = cacheName;
   }

   public RemoveRecoveryInfoCommand() {
   }

   public RemoveRecoveryInfoCommand(long internalId, String cacheName) {
      this.internalId = internalId;
      this.cacheName = cacheName;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (xid != null) {
         recoveryManager.removeRecoveryInformation(xid);
      } else {
         recoveryManager.removeRecoveryInformation(internalId);
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      Object[] result = new Object[2];
      if (xid != null) {
         result[0] = xid;
      } else {
         result[0] = internalId;
      }
      result[1] = cacheName;
      return result;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Wrong command id. Received " + commandId + " and expected " + Ids.REMOVE_RECOVERY_INFO_TX_COMMAND);
      }
      if (parameters[0] instanceof Xid) {
         xid = (Xid) parameters[0];
      } else {
         internalId = (Long) parameters[0];
      }
      cacheName = (String) parameters[1];
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{ xid=" + xid +
            ", internalId=" + internalId +
            ", cacheName=" + cacheName + "} ";
   }
}
