package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessHandler;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * The interceptor in charge of firing off notifications to cache listeners
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 9.0
 */
public class NotificationInterceptor extends DDAsyncInterceptor {
   private CacheNotifier notifier;
   private final InvocationSuccessHandler transactionCompleteSuccessHandler = new InvocationSuccessHandler() {
      @Override
      public void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv)
            throws Throwable {
         boolean successful = !(rCommand instanceof RollbackCommand);
         notifier.notifyTransactionCompleted(((TxInvocationContext) rCtx).getGlobalTransaction(), successful, rCtx);
      }
   };

   @Inject
   public void injectDependencies(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit()) return invokeNext(ctx, command);

      return invokeNext(ctx, command).thenAccept(transactionCompleteSuccessHandler);
   }

   @Override
   public BasicInvocationStage visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNext(ctx, command).thenAccept(transactionCompleteSuccessHandler);
   }

   @Override
   public BasicInvocationStage visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return invokeNext(ctx, command).thenAccept(transactionCompleteSuccessHandler);
   }
}
