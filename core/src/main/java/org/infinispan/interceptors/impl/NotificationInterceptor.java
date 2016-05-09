package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.concurrent.CompletableFuture;

/**
 * The interceptor in charge of firing off notifications to cache listeners
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 9.0
 */
public class NotificationInterceptor extends DDAsyncInterceptor {
   private CacheNotifier notifier;
   private final ReturnHandler transactionCompleteReturnHandler = new ReturnHandler() {
      @Override
      public CompletableFuture<Object> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
            Throwable throwable) throws Throwable {
         if (throwable != null)
            return null;

         boolean successful = !(rCommand instanceof RollbackCommand);
         notifier.notifyTransactionCompleted(((TxInvocationContext) rCtx).getGlobalTransaction(), successful,
               rCtx);
         return null;
      }
   };

   @Inject
   public void injectDependencies(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit())
         return ctx.continueInvocation();

      return ctx.onReturn(transactionCompleteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return ctx.onReturn(transactionCompleteReturnHandler);
   }

   @Override
   public CompletableFuture<Void> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return ctx.onReturn(transactionCompleteReturnHandler);
   }
}
