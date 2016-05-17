package org.infinispan.interceptors.impl;


import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Always at the end of the chain, directly in front of the cache. Simply calls into the cache using reflection. If the
 * call resulted in a modification, add the Modification to the end of the modification list keyed by the current
 * transaction.
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
   public BasicInvocationStage visitCommand(InvocationContext ctx, VisitableCommand command)
         throws Throwable {
      if (trace)
         log.tracef("Invoking: %s", command.getClass().getSimpleName());
      return returnWith(command.perform(ctx));
   }
}
