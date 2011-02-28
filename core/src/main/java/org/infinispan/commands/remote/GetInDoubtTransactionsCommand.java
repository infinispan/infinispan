package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.Ids;

import javax.transaction.xa.Xid;
import java.util.List;

/**
 * Rpc to obtain all in-doubt prepared transactions stored on remote nodes.
 * A transaction is in doubt if it is prepared and the node where it started has crashed.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class GetInDoubtTransactionsCommand extends RecoveryCommand {

   public static final int COMMAND_ID = Ids.GET_IN_DOUBT_TX_COMMAND;

   public GetInDoubtTransactionsCommand() {
   }

   public GetInDoubtTransactionsCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public List<Xid> perform(InvocationContext ctx) throws Throwable {
      return recoveryManager.getLocalInDoubtTransactions();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] {cacheName};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Expected " + COMMAND_ID + "and received " + commandId);
      cacheName = (String) parameters[0];
   }
}
