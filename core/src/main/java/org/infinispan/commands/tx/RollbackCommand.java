package org.infinispan.commands.tx;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * Command corresponding to a transaction rollback.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.ROLLBACK_COMMAND)
public class RollbackCommand extends AbstractTransactionBoundaryCommand {
   public static final byte COMMAND_ID = 13;

   public RollbackCommand(ByteString cacheName, GlobalTransaction globalTransaction) {
      super(-1, cacheName, globalTransaction);
   }

   @ProtoFactory
   public RollbackCommand(int topologyId, ByteString cacheName, GlobalTransaction globalTransaction) {
      super(topologyId, cacheName, globalTransaction);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      // Need to mark the transaction as completed even if the prepare command was not executed on this node
      TransactionTable txTable = registry.getTransactionTableRef().running();
      txTable.markTransactionCompleted(globalTx, false);
      return super.invokeAsync(registry);
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
