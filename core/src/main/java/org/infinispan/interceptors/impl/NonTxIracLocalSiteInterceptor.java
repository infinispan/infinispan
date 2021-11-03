package org.infinispan.interceptors.impl;

import static org.infinispan.commands.SegmentSpecificCommand.extractSegment;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.metadata.impl.IracMetadata;

/**
 * Interceptor used by IRAC for non transactional caches to handle the local site updates.
 * <p>
 * The primary owner job is to generate a new {@link IracMetadata} for the write and store in the {@link WriteCommand}.
 * If the command is successful, the {@link IracMetadata} is stored in the context entry.
 * <p>
 * The backup owners just handle the updates from the primary owner and extract the {@link IracMetadata} to stored in
 * the context entry.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class NonTxIracLocalSiteInterceptor extends AbstractIracLocalSiteInterceptor {

   private final InvocationFinallyAction<WriteCommand> afterWriteCommand = this::handleWriteCommand;

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) {
      return visitWriteCommand(ctx, command);
   }

   private Object visitWriteCommand(InvocationContext ctx, WriteCommand command) {
      if (command.hasAnyFlag(FlagBitSets.IRAC_UPDATE)) {
         return invokeNext(ctx, command);
      }
      for (Object key : command.getAffectedKeys()) {
         visitNonTxKey(ctx, key, command);
      }
      return invokeNextAndFinally(ctx, command, afterWriteCommand);
   }

   /**
    * Visits th {@link WriteCommand} after executed and stores the {@link IracMetadata} if it was successful.
    */
   @SuppressWarnings("unused")
   private void handleWriteCommand(InvocationContext ctx, WriteCommand command, Object rv, Throwable t) {
      if (!command.isSuccessful()) {
         return;
      }
      for (Object key : command.getAffectedKeys()) {
         if (skipEntryCommit(ctx, command, key)) {
            continue;
         }
         setMetadataToCacheEntry(ctx.lookupEntry(key), extractSegment(command, key, keyPartitioner), command.getInternalMetadata(key).iracMetadata());
      }
   }


}
