package org.infinispan.commands;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntryRetrievalCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.*;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.iteration.EntryIterable;

import java.util.Collection;

/**
 * An abstract implementation of a Visitor that delegates all visit calls to a default handler which can be overridden.
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractVisitor implements Visitor {
   // write commands

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }
   
   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   // read commands

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitValuesCommand(InvocationContext ctx, ValuesCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitEntryRetrievalCommand(InvocationContext ctx, EntryRetrievalCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   // tx commands

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
      return handleDefault(ctx, invalidateCommand);
   }
   
   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command) throws Throwable {
	   return visitInvalidateCommand(ctx, invalidateL1Command);
   }

   /**
    * A default handler for all commands visited.  This is called for any visit method called, unless a visit command is
    * appropriately overridden.
    *
    * @param ctx     invocation context
    * @param command command to handle
    * @return return value
    * @throws Throwable in the case of a problem
    */
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return null;
   }

   /**
    * Helper method to visit a collection of VisitableCommands.
    *
    * @param ctx     Invocation context
    * @param toVisit collection of commands to visit
    * @throws Throwable in the event of problems
    */
   public void visitCollection(InvocationContext ctx, Collection<? extends VisitableCommand> toVisit) throws Throwable {
      for (VisitableCommand command : toVisit) {
         command.acceptVisitor(ctx, this);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }
   
   @Override
   public <V> Object visitDistributedExecuteCommand(InvocationContext ctx, DistributedExecuteCommand<V> command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }
}
