package org.infinispan.util.concurrent.locks.deadlock;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Command to probe for deadlock detection.
 *
 * <p>
 * The deadlock detection algorithm utilizes this command to detect cycles during pessimistic locking in transactions. This
 * command has the role of a probe in edge-chasing deadlock detection algorithms. It holds two transactions to verify.
 * The {@link #initiator} is the transaction that starts probing, and the {@link #globalTx} (or holder) locks the resources.
 * </p>
 *
 * <p>
 * The command executes in the transactional context of the {@link #globalTx}. Therefore, the {@link #globalTx} must
 * exist locally. Otherwise, the command does not run since the transaction does not exist locally.
 * </p>
 *
 * @author José Bolina
 */
public class DeadlockProbeCommand extends AbstractTransactionBoundaryCommand implements TopologyAffectedCommand, ReplicableCommand {

   private static final Log log = LogFactory.getLog(DeadlockProbeCommand.class);
   public static final byte COMMAND_ID = 15;

   private GlobalTransaction initiator;

   private DeadlockProbeCommand() {
      super(null); // For command id uniqueness test
   }

   public DeadlockProbeCommand(ByteString cacheName, GlobalTransaction initiator, GlobalTransaction holder) {
      super(cacheName);
      this.initiator = initiator;
      this.globalTx = holder;
   }

   public DeadlockProbeCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitDeadlockProbeCommand((TxInvocationContext) ctx, this);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      InvocationContext ctx = createContext(registry);
      if (ctx == null) {
         log.tracef("No context to run: %s", this);
         return CompletableFutures.completedNull();
      }

      return registry.getInterceptorChain()
            .running()
            .invokeAsync(ctx, this);
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      super.writeTo(output);
      output.writeObject(initiator);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      initiator = (GlobalTransaction) input.readObject();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public GlobalTransaction getInitiator() {
      return initiator;
   }

   private InvocationContext createContext(ComponentRegistry cr) {
      // Try first a local transaction and fallback to remote.
      InvocationContext ctx = createLocalContext(cr);
      if (ctx == null) return createRemoteContext(cr);
      return ctx;
   }

   private TxInvocationContext<LocalTransaction> createLocalContext(ComponentRegistry cr) {
      TransactionTable txTable = getTransactionTable(cr);
      LocalTransaction transaction = txTable.getLocalTransaction(globalTx);

      if (transaction == null) {
         log.tracef("Local transaction is missing (%s)", globalTx);
         return null;
      }

      return cr.getInvocationContextFactory()
            .running()
            .createTxInvocationContext(transaction);
   }

   private TxInvocationContext<RemoteTransaction> createRemoteContext(ComponentRegistry cr) {
      TransactionTable txTable = getTransactionTable(cr);
      RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);

      if (transaction == null) {
         if (!txTable.isTransactionCompleted(globalTx) && Objects.equals(initiator, globalTx)) {
            log.tracef("Receive deadlock before lock command: %s", initiator);
            transaction = txTable.getOrCreateRemoteTransaction(globalTx, Collections.emptyList());
            transaction.markAsDeadlock();
         } else {
            log.tracef("Remote transaction is missing (%s). Not doing deadlock check", globalTx);
            return null;
         }
      }

      return cr.getInvocationContextFactory()
            .running()
            .createRemoteTxInvocationContext(transaction, getOrigin());
   }

   private TransactionTable getTransactionTable(ComponentRegistry cr) {
      return cr.getTransactionTableRef().running();
   }

   @Override
   public String toString() {
      return "DeadlockProbeCommand{" +
            "initiator=" + initiator +
            ", holder=" + globalTx +
            ", cacheName=" + cacheName +
            '}';
   }
}
