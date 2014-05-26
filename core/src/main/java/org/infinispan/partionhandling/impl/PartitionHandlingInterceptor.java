package org.infinispan.partionhandling.impl;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;

public class PartitionHandlingInterceptor extends CommandInterceptor {

   PartitionHandlingManager partitionHandlingManager;

   @Inject void init(PartitionHandlingManager partitionHandlingManager) {
      this.partitionHandlingManager = partitionHandlingManager;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      partitionHandlingManager.checkWrite(command.getKey());
      return super.visitPutKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      partitionHandlingManager.checkWrite(command.getKey());
      return super.visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      partitionHandlingManager.checkWrite(command.getKey());
      return super.visitReplaceCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      for (Object k : command.getAffectedKeys())
         partitionHandlingManager.checkWrite(k);
      return super.visitPutMapCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      partitionHandlingManager.checkClear();
      return super.visitClearCommand(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      partitionHandlingManager.checkWrite(command.getKey());
      return super.visitApplyDeltaCommand(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      partitionHandlingManager.checkRead(command.getKey());
      return super.visitGetKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return super.visitPrepareCommand(ctx, command);    // TODO: Customise this generated block
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return super.visitRollbackCommand(ctx, command);    // TODO: Customise this generated block
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return super.visitCommitCommand(ctx, command);    // TODO: Customise this generated block
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      return super.visitLockControlCommand(ctx, command);    // TODO: Customise this generated block
   }

   @Override
   public <V> Object visitDistributedExecuteCommand(InvocationContext ctx, DistributedExecuteCommand<V> command) throws Throwable {
      return super.visitDistributedExecuteCommand(ctx, command);    // TODO: Customise this generated block
   }
}
