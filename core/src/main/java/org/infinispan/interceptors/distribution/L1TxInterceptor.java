package org.infinispan.interceptors.distribution;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Interceptor that handles L1 logic for transactional caches.
 *
 * @author William Burns
 */
public class L1TxInterceptor extends L1NonTxInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true, true);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // TODO: need to figure out if we do anything here? - is the prepare/commmit L1 invalidation sufficient?
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true, true);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true, false);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true, false);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true, false);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (command.isOnePhaseCommit() && shouldFlushL1(ctx)) {
         return flushL1CachesAndInvokeNext(ctx, command);
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldFlushL1(ctx)) {
        return flushL1CachesAndInvokeNext(ctx, command);
      }
      return invokeNext(ctx, command);
   }

   @Override
   protected boolean skipL1Lookup(FlagAffectedCommand command, Object key) {
      // TODO: need to skip L1 lookups when the command doesn't require the value to be returned like unsafe return values or write skew check ??
      return super.skipL1Lookup(command, key);
   }

   private boolean shouldFlushL1(TxInvocationContext ctx) {
      return !ctx.getAffectedKeys().isEmpty();
   }

   private Object flushL1CachesAndInvokeNext(TxInvocationContext ctx, VisitableCommand command) {
      CompletableFuture<?> f = (CompletableFuture<?>) l1Manager.flushCache(ctx.getAffectedKeys(), ctx.getOrigin(), true);
      if (f != null && !f.isDone()) {
         return asyncInvokeNext(ctx, command, f.exceptionally(throwable -> {
            getLog().failedInvalidatingRemoteCache(throwable);
            throw CompletableFutures.asCompletionException(throwable);
         }));
      } else {
         return invokeNext(ctx, command);
      }
   }

}
