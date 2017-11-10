package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * The interceptor in charge of firing off notifications to cache listeners
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 9.0
 */
public class NotificationInterceptor extends DDAsyncInterceptor {
   @Inject private CacheNotifier notifier;
   private final InvocationSuccessAction commitSuccessAction = new InvocationSuccessAction() {
      @Override
      public void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv)
            throws Throwable {
         notifier.notifyTransactionCompleted(((TxInvocationContext) rCtx).getGlobalTransaction(), true, rCtx);
      }
   };
   private final InvocationSuccessAction rollbackSuccessAction = new InvocationSuccessAction() {
      @Override
      public void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv)
            throws Throwable {
         notifier.notifyTransactionCompleted(((TxInvocationContext) rCtx).getGlobalTransaction(), false, rCtx);
      }
   };

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (!command.isOnePhaseCommit()) return invokeNext(ctx, command);

      return invokeNextThenAccept(ctx, command, commitSuccessAction);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, commitSuccessAction);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, rollbackSuccessAction);
   }
}
