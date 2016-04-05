package org.infinispan.interceptors.impl;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.concurrent.CompletableFuture;

/**
 * The interceptor in charge of firing off notifications to cache listeners
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 9.0
 */
public class NotificationInterceptor extends DDSequentialInterceptor {
   private CacheNotifier notifier;

   @Inject
   public void injectDependencies(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retval = ctx.forkInvocationSync(command);
      if (command.isOnePhaseCommit()) notifier.notifyTransactionCompleted(ctx.getGlobalTransaction(), true, ctx);
      return ctx.shortCircuit(retval);
   }

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      Object retval = ctx.forkInvocationSync(command);
      notifier.notifyTransactionCompleted(ctx.getGlobalTransaction(), true, ctx);
      return ctx.shortCircuit(retval);
   }

   @Override
   public CompletableFuture<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      Object retval = ctx.forkInvocationSync(command);
      notifier.notifyTransactionCompleted(ctx.getGlobalTransaction(), false, ctx);
      return ctx.shortCircuit(retval);
   }
}
