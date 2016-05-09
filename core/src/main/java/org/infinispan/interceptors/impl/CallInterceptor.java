package org.infinispan.interceptors.impl;


import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection. If the
 * call resulted in a modification, add the Modification to the end of the modification list keyed by the current
 * transaction.
 *
 *
 * @author Bela Ban
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 9.0
 */
public class CallInterceptor extends BaseAsyncInterceptor {
   private static final Log log = LogFactory.getLog(CallInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public CompletableFuture<Void> visitCommand(InvocationContext ctx, VisitableCommand command)
         throws Throwable {
      if (command instanceof AbstractTransactionBoundaryCommand) {
         if (trace)
            log.tracef("Suppressing invocation for %s", command.getClass().getSimpleName());
         return ctx.shortCircuit(null);
      }

      if (trace)
         log.tracef("Invoking: %s", command.getClass().getSimpleName());
      return ctx.shortCircuit(command.perform(ctx));
   }
}