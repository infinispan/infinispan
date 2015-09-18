package org.infinispan.interceptors.base;

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

import java.util.concurrent.CompletableFuture;

/**
 * An interceptor in the new sequential invocation chain.
 *
 * @author Dan Berindei
 * @since 8.1
 */
public interface DoubleDispatchSequentialInterceptor extends SequentialInterceptor, Visitor {

   @Override
   default CompletableFuture<Object> visitCommand(InvocationContext ctx, VisitableCommand cmd)
         throws Throwable {
      return (CompletableFuture<Object>) cmd.acceptVisitor(ctx, this);
   }

   @Override
   CompletableFuture<Object> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitRemoveCommand(InvocationContext ctx,
                                                RemoveCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitPutMapCommand(InvocationContext ctx,
                                                PutMapCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitGetAllCommand(InvocationContext ctx,
                                                GetAllCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitKeySetCommand(InvocationContext ctx,
                                                KeySetCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws
         Throwable;

   @Override
   CompletableFuture<Object> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws
         Throwable;

   @Override
   CompletableFuture<Object> visitInvalidateCommand(InvocationContext ctx,
                                                    InvalidateCommand invalidateCommand)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitInvalidateL1Command(InvocationContext ctx,
                                                      InvalidateL1Command invalidateL1Command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable;

   @Override
   <V> Object visitDistributedExecuteCommand(InvocationContext ctx, DistributedExecuteCommand<V> command) throws Throwable;

   @Override
   CompletableFuture<Object> visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable;

   @Override
   CompletableFuture<Object> visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitReadWriteKeyValueCommand(InvocationContext ctx,
                                                           ReadWriteKeyValueCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
                                                              WriteOnlyManyEntriesCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitWriteOnlyKeyValueCommand(InvocationContext ctx,
                                                           WriteOnlyKeyValueCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable;

   @Override
   CompletableFuture<Object> visitReadWriteManyEntriesCommand(InvocationContext ctx,
                                                              ReadWriteManyEntriesCommand command)
         throws Throwable;
}
