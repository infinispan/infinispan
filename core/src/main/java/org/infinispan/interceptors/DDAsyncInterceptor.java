package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * Interface for async interceptors using double-dispatch.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public abstract class DDAsyncInterceptor extends BaseAsyncInterceptor implements Visitor {
   @SuppressWarnings("unchecked")
   @Override
   public final BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return (BasicInvocationStage) command.acceptVisitor(ctx, this);
   }

   protected BasicInvocationStage handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public <V> BasicInvocationStage visitDistributedExecuteCommand(InvocationContext ctx,
         DistributedExecuteCommand<V> command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
         WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteManyEntriesCommand(InvocationContext ctx,
         ReadWriteManyEntriesCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }
}
