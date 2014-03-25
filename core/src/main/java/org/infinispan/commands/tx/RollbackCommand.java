package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Command corresponding to a transaction rollback.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class RollbackCommand extends AbstractTransactionBoundaryCommand {
   public static final byte COMMAND_ID = 13;

   private RollbackCommand() {
      super(null); // For command id uniqueness test
   }

   public RollbackCommand(String cacheName, GlobalTransaction globalTransaction) {
      super(cacheName);
      this.globalTx = globalTransaction;
   }

   public RollbackCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // Need to mark the transaction as completed even if the prepare command was not executed on this node
      txTable.markTransactionCompleted(globalTx);
      return super.perform(ctx);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRollbackCommand((TxInvocationContext) ctx, this);
   }

   @Override
   public void visitRemoteTransaction(RemoteTransaction tx) {
      tx.markForRollback(true);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "RollbackCommand {" + super.toString();
   }
}
