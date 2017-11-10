package org.infinispan.interceptors.impl;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor that captures batched calls and attaches contexts.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 9.0
 */
public class BatchingInterceptor extends DDAsyncInterceptor {
   @Inject private BatchContainer batchContainer;
   @Inject private TransactionManager transactionManager;
   @Inject private InvocationContextFactory invocationContextFactory;

   private static final Log log = LogFactory.getLog(BatchingInterceptor.class);

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // eviction is non-tx, so this interceptor should be no-op for EvictCommands
      return invokeNext(ctx, command);
   }

   /**
    * Simply check if there is an ongoing tx. <ul> <li>If there is one, this is a no-op and just passes the call up the
    * chain.</li> <li>If there isn't one and there is a batch in progress, resume the batch's tx, pass up, and finally
    * suspend the batch's tx.</li> <li>If there is no batch in progress, just pass the call up the chain.</li> </ul>
    */
   @Override
   public Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         // Nothing to do for remote calls
         return invokeNext(ctx, command);
      }

      Transaction tx;
      if (transactionManager.getTransaction() != null || (tx = batchContainer.getBatchTransaction()) == null) {
         // The active transaction means we are in an auto-batch.
         // No batch means a read-only auto-batch.
         // Either way, we don't need to do anything
         return invokeNext(ctx, command);
      }

      try {
         transactionManager.resume(tx);
         if (ctx.isInTxScope()) {
            return invokeNext(ctx, command);
         }

         log.tracef("Called with a non-tx invocation context: %s", ctx);
         InvocationContext txInvocationContext = invocationContextFactory.createInvocationContext(true, -1);
         return invokeNext(txInvocationContext, command);
      } finally {
         suspendTransaction();
      }
   }

   private void suspendTransaction() throws SystemException {
      if (transactionManager.getTransaction() != null && batchContainer.isSuspendTxAfterInvocation())
         transactionManager.suspend();
   }
}
