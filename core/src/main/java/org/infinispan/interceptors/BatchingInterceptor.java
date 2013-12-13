package org.infinispan.interceptors;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Interceptor that captures batched calls and attaches contexts.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class BatchingInterceptor extends CommandInterceptor {
   private BatchContainer batchContainer;
   private TransactionManager transactionManager;
   private InvocationContextFactory invocationContextFactory;

   private static final Log log = LogFactory.getLog(BatchingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   private void inject(BatchContainer batchContainer, TransactionManager transactionManager,
                       InvocationContextFactory invocationContextFactory) {
      this.batchContainer = batchContainer;
      this.transactionManager = transactionManager;
      this.invocationContextFactory = invocationContextFactory;
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // eviction is non-tx, so this interceptor should be no-op for EvictCommands
      return invokeNextInterceptor(ctx, command);
   }

   /**
    * Simply check if there is an ongoing tx. <ul> <li>If there is one, this is a no-op and just passes the call up the
    * chain.</li> <li>If there isn't one and there is a batch in progress, resume the batch's tx, pass up, and finally
    * suspend the batch's tx.</li> <li>If there is no batch in progress, just pass the call up the chain.</li> </ul>
    */
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         // Nothing to do for remote calls
         return invokeNextInterceptor(ctx, command);
      }

      Transaction tx;
      if (transactionManager.getTransaction() != null || (tx = batchContainer.getBatchTransaction()) == null) {
         // The active transaction means we are in an auto-batch.
         // No batch means a read-only auto-batch.
         // Either way, we don't need to do anything
         return invokeNextInterceptor(ctx, command);
      }

      try {
         transactionManager.resume(tx);
         InvocationContext invocationContext = ctx;
         if (!ctx.isInTxScope()) {
            // If there's no ongoing tx then BatchingInterceptor creates one and then invokes next interceptor,
            // so that all interceptors in the stack will be executed in a transactional context.
            // This is where a new context (TxInvocationContext) is created, as the existing context is not transactional: NonTxInvocationContext.
            log.tracef("Called with a non-tx invocation context: %s", ctx);
            invocationContext = invocationContextFactory.createInvocationContext(true, -1);
         }
         return invokeNextInterceptor(invocationContext, command);
      } finally {
         if (transactionManager.getTransaction() != null && batchContainer.isSuspendTxAfterInvocation())
            transactionManager.suspend();
      }
   }
}
