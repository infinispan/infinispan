package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;

/**
 * The interceptor in charge of firing off notifications to cache listeners
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 9.0
 */
public class NotificationInterceptor extends DDAsyncInterceptor {
   @Inject private CacheNotifier notifier;
   private final InvocationSuccessFunction commitSuccessAction = new InvocationSuccessFunction() {
      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv)
            throws Throwable {
         return delayedValue(notifier.notifyTransactionCompleted(((TxInvocationContext) rCtx).getGlobalTransaction(), true, rCtx), rv);
      }
   };
   private final InvocationSuccessFunction rollbackSuccessAction = new InvocationSuccessFunction() {
      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv)
            throws Throwable {
         return delayedValue(notifier.notifyTransactionCompleted(((TxInvocationContext) rCtx).getGlobalTransaction(), false, rCtx), rv);
      }
   };

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit() || !notifier.hasListener(TransactionCompleted.class)) {
         return invokeNext(ctx, command);
      }

      return invokeNextThenApply(ctx, command, commitSuccessAction);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (!notifier.hasListener(TransactionCompleted.class)) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, commitSuccessAction);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!notifier.hasListener(TransactionCompleted.class)) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, rollbackSuccessAction);
   }
}
