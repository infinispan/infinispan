package org.infinispan.statetransfer;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.*;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

import java.util.Collections;
import java.util.HashSet;

/**
 * // TODO: Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class AffectedKeysVisitor extends AbstractVisitor {

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      return command.getAffectedKeys();
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      return ctx.getAffectedKeys();
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
      return ctx.getAffectedKeys();
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) {
      return new HashSet<Object>(command.getKeys());
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return command.getAffectedKeys();
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return command.getAffectedKeys();
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) {
      return Collections.singleton(command.getKey());
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      return command.getAffectedKeys();
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      return command.getAffectedKeys();
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      return command.getAffectedKeys();
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) {
      return null;
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) {
      return null;
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) {
      return null;
   }
}
