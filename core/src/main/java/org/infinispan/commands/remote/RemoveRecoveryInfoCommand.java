package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.Ids;

import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.List;

/**
 * Command for removing recovery related information from the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RemoveRecoveryInfoCommand extends RecoveryCommand {

   public static final int COMMAND_ID = Ids.REMOVE_RECOVERY_INFO_TX_COMMAND;

   private volatile List<Xid> xids;

   public RemoveRecoveryInfoCommand(List<Xid> xids, String cacheName) {
      this.xids = xids;
      this.cacheName = cacheName;
   }

   public RemoveRecoveryInfoCommand() {
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      recoveryManager.removeLocalRecoveryInformation(xids);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      Object[] result = new Object[2];
      result[0] = xids;
      result[1] = cacheName;
      return result;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Wrong command id. Received " + commandId + " and expected " + Ids.REMOVE_RECOVERY_INFO_TX_COMMAND);
      }
      xids = (List<Xid>) parameters[0];
      cacheName = (String) parameters[1];
   }

   @Override
   public String toString() {
      return "ForgetTxCommand{" +
            "xids=" + (xids == null ? null : Arrays.asList(xids)) +
            "} " + super.toString();
   }
}
