package org.infinispan.commands.tx;

import static org.infinispan.commons.util.InfinispanCollections.forEach;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.locks.TransactionalRemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Command corresponding to the 1st phase of 2PC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PrepareCommand extends AbstractTransactionBoundaryCommand implements TransactionalRemoteLockCommand {

   private static final Log log = LogFactory.getLog(PrepareCommand.class);
   private static boolean trace = log.isTraceEnabled();

   public static final byte COMMAND_ID = 12;

   protected WriteCommand[] modifications;
   protected boolean onePhaseCommit;
   protected CacheNotifier notifier;
   protected RecoveryManager recoveryManager;
   private transient boolean replayEntryWrapping  = false;
   protected boolean retriedCommand;

   private static final WriteCommand[] EMPTY_WRITE_COMMAND_ARRAY = new WriteCommand[0];

   public void initialize(CacheNotifier notifier, RecoveryManager recoveryManager) {
      this.notifier = notifier;
      this.recoveryManager = recoveryManager;
   }

   private PrepareCommand() {
      super(null); // For command id uniqueness test
   }

   public PrepareCommand(ByteString cacheName, GlobalTransaction gtx, boolean onePhaseCommit, WriteCommand... modifications) {
      super(cacheName);
      this.globalTx = gtx;
      this.modifications = modifications;
      this.onePhaseCommit = onePhaseCommit;
   }

   public PrepareCommand(ByteString cacheName, GlobalTransaction gtx, List<WriteCommand> commands, boolean onePhaseCommit) {
      super(cacheName);
      this.globalTx = gtx;
      this.modifications = commands == null || commands.isEmpty() ? null : commands.toArray(new WriteCommand[commands.size()]);
      this.onePhaseCommit = onePhaseCommit;
   }

   public PrepareCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      RemoteTxInvocationContext ctx = createContext();
      if (ctx == null) {
         return CompletableFutures.completedNull();
      }

      if (trace)
         log.tracef("Invoking remotely originated prepare: %s with invocation context: %s", this, ctx);
      CompletionStage<Void> stage = null;
      if (notifier.hasListener(TransactionRegistered.class)) {
         stage = notifier.notifyTransactionRegistered(ctx.getGlobalTransaction(), false);
      }

      if (stage == null || CompletionStages.isCompleteSuccessfully(stage)) {
         return invoker.invokeAsync(ctx, this);
      } else {
         return stage.thenCompose(v -> invoker.invokeAsync(ctx, this)).toCompletableFuture();
      }
   }

   @Override
   public RemoteTxInvocationContext createContext() {
      if (recoveryManager != null && recoveryManager.isTransactionPrepared(globalTx)) {
         log.tracef("The transaction %s is already prepared. Skipping prepare call.", globalTx);
         return null;
      }

      // 1. first create a remote transaction (or get the existing one)
      RemoteTransaction remoteTransaction = txTable.getOrCreateRemoteTransaction(globalTx, modifications);
      //set the list of modifications anyway, as the transaction might have already been created by a previous
      //LockControlCommand with null modifications.
      if (hasModifications()) {
         remoteTransaction.setModifications(Arrays.asList(modifications));
      }

      // 2. then set it on the invocation context
      return icf.createRemoteTxInvocationContext(remoteTransaction, getOrigin());
   }

   @Override
   public Collection<?> getKeysToLock() {
      if (modifications == null || modifications.length == 0) {
         return Collections.emptyList();
      }
      final Set<Object> set = new HashSet<>(modifications.length);
      forEach(modifications, writeCommand -> {
         if (writeCommand.hasAnyFlag(FlagBitSets.SKIP_LOCKING)) {
            return;
         }
         switch (writeCommand.getCommandId()) {
            case PutKeyValueCommand.COMMAND_ID:
            case RemoveCommand.COMMAND_ID:
            case ComputeCommand.COMMAND_ID:
            case ComputeIfAbsentCommand.COMMAND_ID:
            case RemoveExpiredCommand.COMMAND_ID:
            case ReplaceCommand.COMMAND_ID:
            case ReadWriteKeyCommand.COMMAND_ID:
            case ReadWriteKeyValueCommand.COMMAND_ID:
            case WriteOnlyKeyCommand.COMMAND_ID:
            case WriteOnlyKeyValueCommand.COMMAND_ID:
               set.add(((DataWriteCommand) writeCommand).getKey());
               break;
            case PutMapCommand.COMMAND_ID:
            case ReadWriteManyCommand.COMMAND_ID:
            case ReadWriteManyEntriesCommand.COMMAND_ID:
            case WriteOnlyManyCommand.COMMAND_ID:
            case WriteOnlyManyEntriesCommand.COMMAND_ID:
               set.addAll(writeCommand.getAffectedKeys());
               break;
            default:
               break;
         }
      });
      return set;
   }

   @Override
   public Object getKeyLockOwner() {
      return globalTx;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return false;
   }

   @Override
   public boolean hasSkipLocking() {
      return false;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPrepareCommand((TxInvocationContext) ctx, this);
   }

   public WriteCommand[] getModifications() {
      return modifications == null ? EMPTY_WRITE_COMMAND_ARRAY : modifications;
   }

   public boolean isOnePhaseCommit() {
      return onePhaseCommit;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      super.writeTo(output); //global tx
      output.writeBoolean(onePhaseCommit);
      output.writeBoolean(retriedCommand);
      MarshallUtil.marshallArray(modifications, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      onePhaseCommit = input.readBoolean();
      retriedCommand = input.readBoolean();
      modifications = MarshallUtil.unmarshallArray(input, WriteCommand[]::new);
   }

   public PrepareCommand copy() {
      PrepareCommand copy = new PrepareCommand(cacheName);
      copy.globalTx = globalTx;
      copy.modifications = modifications == null ? null : modifications.clone();
      copy.onePhaseCommit = onePhaseCommit;
      return copy;
   }

   @Override
   public String toString() {
      return "PrepareCommand {" +
            "modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
            ", onePhaseCommit=" + onePhaseCommit +
            ", retried=" + retriedCommand +
            ", " + super.toString();
   }

   public boolean hasModifications() {
      return modifications != null && modifications.length > 0;
   }

   public Collection<?> getAffectedKeys() {
      if (modifications == null || modifications.length == 0)
         return Collections.emptySet();

      if (modifications.length == 1) return modifications[0].getAffectedKeys();
      Set<Object> keys = new HashSet<>(modifications.length);
      for (WriteCommand wc: modifications) keys.addAll(wc.getAffectedKeys());
      return keys;
   }

   /**
    * If set to true, then the keys touched by this transaction are to be wrapped again and original ones discarded.
    */
   public boolean isReplayEntryWrapping() {
      return replayEntryWrapping;
   }

   /**
    * @see #isReplayEntryWrapping()
    */
   public void setReplayEntryWrapping(boolean replayEntryWrapping) {
      this.replayEntryWrapping = replayEntryWrapping;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   public boolean isRetriedCommand() {
      return retriedCommand;
   }

   public void setRetriedCommand(boolean retriedCommand) {
      this.retriedCommand = retriedCommand;
   }
}
