package org.infinispan.commands.remote.recovery;

import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Rpc to obtain all in-doubt prepared transactions stored on remote nodes.
 * A transaction is in doubt if it is prepared and the node where it started has crashed.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class GetInDoubtTransactionsCommand extends RecoveryCommand {

   private static final Log log = LogFactory.getLog(GetInDoubtTransactionsCommand.class);

   public static final int COMMAND_ID = 21;

   private GetInDoubtTransactionsCommand() {
      super(null); // For command id uniqueness test
   }

   public GetInDoubtTransactionsCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public List<Xid> perform(InvocationContext ctx) throws Throwable {
      List<Xid> localInDoubtTransactions = recoveryManager.getInDoubtTransactions();
      log.tracef("Returning result %s", localInDoubtTransactions);
      return localInDoubtTransactions;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      // No parameters
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      // No parameters
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " { cacheName = " + cacheName + "}";
   }
}
