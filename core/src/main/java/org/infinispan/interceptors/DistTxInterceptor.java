package org.infinispan.interceptors;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

import java.util.HashMap;
import java.util.Map;

/**
 * A special form of the TxInterceptor that is aware of distribution and consistent hashing, and as such only replays
 * methods during a remote prepare that are targeted to this specific cache instance.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistTxInterceptor extends TxInterceptor {

   DistributionManager dm;
   ReplayCommandVisitor replayCommandVisitor = new ReplayCommandVisitor();
   private CommandsFactory commandsFactory;

   @Inject
   public void injectDistributionManager(DistributionManager dm, CommandsFactory commandsFactory) {
      this.dm = dm;
      this.commandsFactory = commandsFactory;
   }

   /**
    * Only replays modifications that are
    */
   @Override
   protected VisitableCommand getCommandToReplay(VisitableCommand command) {
      try {
         return (VisitableCommand) command.acceptVisitor(null, replayCommandVisitor);
      } catch (RuntimeException re) {
         throw re;
      } catch (Throwable th) {
         throw new RuntimeException(th);
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand cmd) throws Throwable {
      dm.getTransactionLogger().logIfNeeded(cmd);
      return super.visitPrepareCommand(ctx, cmd);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand cmd) throws Throwable {
      dm.getTransactionLogger().logIfNeeded(cmd);
      return super.visitRollbackCommand(ctx, cmd);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand cmd) throws Throwable {
      dm.getTransactionLogger().logIfNeeded(cmd);
      return super.visitCommitCommand(ctx, cmd);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object o = super.visitPutKeyValueCommand(ctx, command);
      if (!ctx.isInTxScope() && command.isSuccessful()) dm.getTransactionLogger().logIfNeeded(command);
      return o;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object o = super.visitRemoveCommand(ctx, command);
      if (!ctx.isInTxScope() && command.isSuccessful()) dm.getTransactionLogger().logIfNeeded(command);
      return o;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object o = super.visitReplaceCommand(ctx, command);
      if (!ctx.isInTxScope() && command.isSuccessful()) dm.getTransactionLogger().logIfNeeded(command);
      return o;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      Object o = super.visitClearCommand(ctx, command);
      if (!ctx.isInTxScope() && command.isSuccessful()) dm.getTransactionLogger().logIfNeeded(command);
      return o;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object o = super.visitPutMapCommand(ctx, command);
      if (!ctx.isInTxScope() && command.isSuccessful()) dm.getTransactionLogger().logIfNeeded(command);
      return o;
   }


   class ReplayCommandVisitor extends AbstractVisitor {
      @Override
      public Object visitPutMapCommand(InvocationContext ignored, PutMapCommand command) {
         Map newMap = new HashMap();
         for (Map.Entry entry : command.getMap().entrySet()) {
            if (dm.isLocal(entry.getKey())) newMap.put(entry.getKey(), entry.getValue());
         }

         if (newMap.isEmpty()) return null;
         if (newMap.size() == command.getMap().size()) return command;
         return commandsFactory.buildPutMapCommand(newMap, command.getLifespanMillis(), command.getMaxIdleTimeMillis());
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ignored, PutKeyValueCommand command) {
         return visitDataWriteCommand(command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ignored, RemoveCommand command) {
         return visitDataWriteCommand(command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ignored, ReplaceCommand command) {
         return visitDataWriteCommand(command);
      }

      private VisitableCommand visitDataWriteCommand(DataWriteCommand command) {
         return dm.isLocal(command.getKey()) ? command : null;
      }

      @Override
      public Object handleDefault(InvocationContext ignored, VisitableCommand command) {
         return command;
      }
   }
}
