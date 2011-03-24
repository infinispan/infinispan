package org.infinispan.commands.remote;

import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.Ids;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private static Log log = LogFactory.getLog(GetInDoubtTransactionsCommand.class);

   public static final int COMMAND_ID = Ids.GET_IN_DOUBT_TX_COMMAND;

   public GetInDoubtTransactionsCommand() {
   }

   public GetInDoubtTransactionsCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public List<Xid> perform(InvocationContext ctx) throws Throwable {
      List<Xid> localInDoubtTransactions = recoveryManager.getLocalInDoubtTransactions();
      if (log.isTraceEnabled()) log.trace("Returning result %s", localInDoubtTransactions);
      return localInDoubtTransactions;
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

   @Override
   public String toString() {
      return getClass().getSimpleName() + " { cacheName = " + cacheName + "}";
   }
}
