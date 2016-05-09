package org.infinispan.interceptors.impl;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.concurrent.CompletableFuture;

/**
 * Interceptor that captures batched calls and attaches contexts.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 9.0
 */
public class BatchingInterceptor extends DDSequentialInterceptor {
   private BatchContainer batchContainer;
   private TransactionManager transactionManager;
   private InvocationContextFactory invocationContextFactory;
   private SequentialInterceptorChain invoker;

   private static final Log log = LogFactory.getLog(BatchingInterceptor.class);

   @Inject
   private void inject(BatchContainer batchContainer, TransactionManager transactionManager,
                       InvocationContextFactory invocationContextFactory, SequentialInterceptorChain invoker) {
      this.batchContainer = batchContainer;
      this.transactionManager = transactionManager;
      this.invocationContextFactory = invocationContextFactory;
      this.invoker = invoker;
   }

   @Override
   public CompletableFuture<Void> visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // eviction is non-tx, so this interceptor should be no-op for EvictCommands
      return ctx.continueInvocation();
   }

   /**
    * Simply check if there is an ongoing tx. <ul> <li>If there is one, this is a no-op and just passes the call up the
    * chain.</li> <li>If there isn't one and there is a batch in progress, resume the batch's tx, pass up, and finally
    * suspend the batch's tx.</li> <li>If there is no batch in progress, just pass the call up the chain.</li> </ul>
    */
   @Override
   public CompletableFuture<Void> handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         // Nothing to do for remote calls
         return ctx.continueInvocation();
      }

      Transaction tx;
      if (transactionManager.getTransaction() != null || (tx = batchContainer.getBatchTransaction()) == null) {
         // The active transaction means we are in an auto-batch.
         // No batch means a read-only auto-batch.
         // Either way, we don't need to do anything
         return ctx.continueInvocation();
      }

      ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (transactionManager.getTransaction() != null && batchContainer.isSuspendTxAfterInvocation())
            transactionManager.suspend();
         return null;
      });

      transactionManager.resume(tx);
      if (ctx.isInTxScope()) {
         return ctx.continueInvocation();
      }

      log.tracef("Called with a non-tx invocation context: %s", ctx);
      InvocationContext txInvocationContext = invocationContextFactory.createInvocationContext(true, -1);
      // Before sequential interceptors, we could continue the invocation with the next interceptor,
      // with invokeNextInterceptor(txInvocationContext, command).
      // But now we keep track of the invocation state (e.g. the current interceptor) in the invocation
      // context itself (BaseSequentialInvocationContext, to be precise), so we have to restart the
      // invocation with the new context instance.
      // TODO Move the creation of the proper invocation context out of the interceptor and into CacheImpl
      return invoker.invokeAsync(txInvocationContext, command).thenCompose(ctx::shortCircuit);
   }
}
