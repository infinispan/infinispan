package org.infinispan.commands.remote.recovery;

import org.infinispan.context.InvocationContext;
import javax.transaction.xa.Xid;

/**
 * Command used by the recovery tooling for forcing transaction completion .
 *
 * @author Mircea Markus
 * @since 5.0
 */
public class CompleteTransactionCommand extends RecoveryCommand {

   public static final byte COMMAND_ID = 24;

   /**
    * The tx which we want to complete.
    */
   private Xid xid;

   /**
    * if true the transaction is committed, otherwise it is rolled back.
    */
   private boolean commit;

   private CompleteTransactionCommand() {
      super(null); // For command id uniqueness test
   }

   public CompleteTransactionCommand(String cacheName) {
      super(cacheName);
   }

   public CompleteTransactionCommand(String cacheName, Xid xid, boolean commit) {
      super(cacheName);
      this.xid = xid;
      this.commit = commit;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return recoveryManager.forceTransactionCompletion(xid, commit);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{xid, commit};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("Unexpected command id: " + commandId + ". Expected " + COMMAND_ID);
      }
      int i = 0;
      xid = (Xid) parameters[i++];
      commit = (Boolean) parameters[i++];
   }

   @Override
   public boolean canBlock() {
      //this command performs the 2PC commit.
      return true;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() +
            "{ xid=" + xid +
            ", commit=" + commit +
            ", cacheName=" + cacheName +
            "} ";
   }
}
