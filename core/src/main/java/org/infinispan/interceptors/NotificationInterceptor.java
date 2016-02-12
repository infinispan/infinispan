package org.infinispan.interceptors;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The interceptor in charge of firing off notifications to cache listeners
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @deprecated Since 8.2, no longer public API.
 */
@Deprecated
public class NotificationInterceptor extends CommandInterceptor {
   private CacheNotifier notifier;

   private static final Log log = LogFactory.getLog(NotificationInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit()) notifier.notifyTransactionCompleted(ctx.getGlobalTransaction(), true, ctx);
      return retval;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      notifier.notifyTransactionCompleted(ctx.getGlobalTransaction(), true, ctx);
      return retval;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      notifier.notifyTransactionCompleted(ctx.getGlobalTransaction(), false, ctx);
      return retval;
   }
}
