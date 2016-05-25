package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.distribution.ConcurrentChangeException;

import java.util.concurrent.CompletableFuture;

/**
 * Used in @{link org.infinispan.configuration.cache.CacheMode#SCATTERED_SYNC scattered cache}
 * The commit is executed in {@link org.infinispan.interceptors.distribution.ScatteringInterceptor}
 * before replicating the change from primary owner.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RetryingEntryWrappingInterceptor extends BaseEntryWrappingInterceptor {

   private final ForkReturnHandler dataWriteReturnHandler = (rCtx, rCommand, rv, throwable) -> {
      if (throwable instanceof ConcurrentChangeException) {
         log.trace("%s has thrown %s, retrying", rCommand, throwable);
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         rCtx.removeLookedUpEntry(dataWriteCommand.getKey());
         return visitCommand(rCtx, dataWriteCommand);
      } else if (throwable != null) {
         throw throwable;
      }
      return rCtx.shortCircuit(rv);
   };

   private final ForkReturnHandler manyWriteReturnHandler = (rCtx, rCommand, rv, throwable) -> {
      if (throwable instanceof ConcurrentChangeException) {
         log.trace("%s has thrown %s, retrying", rCommand, throwable);
         WriteCommand writeCommand = (WriteCommand) rCommand;
         for (Object key : writeCommand.getAffectedKeys()) {
            rCtx.removeLookedUpEntry(key);
         }
         return visitCommand(rCtx, writeCommand);
      } else if (throwable != null) {
         throw throwable;
      }
      return rCtx.shortCircuit(rv);
   };


   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      super.visitPutKeyValueCommand(ctx, command);
      return ctx.forkInvocation(command, dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      super.visitRemoveCommand(ctx, command);
      return ctx.forkInvocation(command, dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      super.visitReplaceCommand(ctx, command);
      return ctx.forkInvocation(command, dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      super.visitPutMapCommand(ctx, command);
      return ctx.forkInvocation(command, manyWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      super.visitWriteOnlyKeyCommand(ctx, command);
      return ctx.forkInvocation(command, dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      super.visitReadWriteKeyValueCommand(ctx, command);
      return ctx.forkInvocation(command, dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      super.visitReadWriteKeyCommand(ctx, command);
      return ctx.forkInvocation(command, dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      super.visitWriteOnlyManyEntriesCommand(ctx, command);
      return ctx.forkInvocation(command, manyWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      super.visitWriteOnlyManyCommand(ctx, command);
      return ctx.forkInvocation(command, manyWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      super.visitWriteOnlyKeyValueCommand(ctx, command);
      return ctx.forkInvocation(command, dataWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      super.visitReadWriteManyCommand(ctx, command);
      return ctx.forkInvocation(command, manyWriteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      super.visitReadWriteManyEntriesCommand(ctx, command);
      return ctx.forkInvocation(command, manyWriteReturnHandler);
   }

   @Override
   protected CompletableFuture<Void> handleDataWriteReturn(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      return null; // return value ignored;
   }

   @Override
   protected CompletableFuture<Void> handleManyWriteReturn(InvocationContext ctx) throws Throwable {
      return null; // return value ignored;
   }
}
