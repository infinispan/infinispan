package org.infinispan.interceptors.sequential;

import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Interceptor that handles L1 logic for transactional caches.
 *
 * @author William Burns
 * @since 6.0
 */
public class L1TxInterceptor extends L1NonTxInterceptor {
   private static final Log log = LogFactory.getLog(L1TxInterceptor.class);

   @Override
   public CompletableFuture<Object> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true, true);
   }

   @Override
   public CompletableFuture<Object> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // TODO: need to figure out if we do anything here? - is the prepare/commmit L1 invalidation sufficient?
      return null;
   }

   @Override
   public CompletableFuture<Object> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true, true);
   }

   @Override
   public CompletableFuture<Object> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true, false);
   }

   @Override
   public CompletableFuture<Object> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (command.isOnePhaseCommit() && shouldFlushL1(ctx)) {
         blockOnL1FutureIfNeeded(flushL1Caches(ctx));
      }

      return null;
   }

   @Override
   public CompletableFuture<Object> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldFlushL1(ctx)) {
         blockOnL1FutureIfNeeded(flushL1Caches(ctx));
      }
      return null;
   }

   @Override
   protected boolean skipL1Lookup(LocalFlagAffectedCommand command, Object key) {
      // TODO: need to skip L1 lookups when the command doesn't require the value to be returned like unsafe return values or write skew check ??
      return super.skipL1Lookup(command, key);
   }

   private boolean shouldFlushL1(TxInvocationContext ctx) {
      return !ctx.getAffectedKeys().isEmpty();
   }

   private Future<?> flushL1Caches(TxInvocationContext ctx) {
      return l1Manager.flushCache(ctx.getAffectedKeys(), ctx.getOrigin(), true);
   }

   private void blockOnL1FutureIfNeeded(Future<?> f) {
      if (f != null && cacheConfiguration.transaction().syncCommitPhase()) {
         try {
            f.get();
         } catch (Exception e) {
            // Ignore SuspectExceptions - if the node has gone away then there is nothing to invalidate anyway.
            if (!(e.getCause() instanceof SuspectException)) {
               log.failedInvalidatingRemoteCache(e);
            }
         }
      }
   }
}
