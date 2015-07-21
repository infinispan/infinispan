package org.infinispan.commands;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetAllCommand;
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
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */

public interface Visitor {
   // write commands

   Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable;

   Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable;

   Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable;

   Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable;

   Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable;

   Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable;
   
   Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable;

   // read commands

   Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable;

   Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable;

   Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable;

   Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable;

   Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable;

   Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable;

   // tx commands

   Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable;

   Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable;

   Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable;

   Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable;
   
   Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command) throws Throwable;

   // locking commands
   Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable;

   Object visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable;

   <V> Object visitDistributedExecuteCommand(InvocationContext ctx, DistributedExecuteCommand<V> command) throws Throwable;

   Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable;

}
