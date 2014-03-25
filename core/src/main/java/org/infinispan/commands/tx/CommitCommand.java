package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Command corresponding to the 2nd phase of 2PC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class CommitCommand extends AbstractTransactionBoundaryCommand {
   public static final byte COMMAND_ID = 14;

   private CommitCommand() {
      super(null); // For command id uniqueness test
   }

   public CommitCommand(String cacheName, GlobalTransaction gtx) {
      super(cacheName);
      this.globalTx = gtx;
   }

   public CommitCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitCommitCommand((TxInvocationContext) ctx, this);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "CommitCommand {" + super.toString();
   }
}
