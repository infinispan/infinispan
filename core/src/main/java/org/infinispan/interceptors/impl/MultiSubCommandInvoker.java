package org.infinispan.interceptors.impl;

import java.util.Iterator;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.InvocationSuccessFunction;

/**
 * Invoke a sequence of sub-commands.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class MultiSubCommandInvoker implements InvocationSuccessFunction {
   private final BaseAsyncInterceptor interceptor;
   private final Object finalStage;
   private final Iterator<VisitableCommand> subCommands;

   private MultiSubCommandInvoker(BaseAsyncInterceptor interceptor, Object finalReturnValue,
                                  Iterator<VisitableCommand> subCommands) {
      this.interceptor = interceptor;
      this.finalStage = finalReturnValue;
      this.subCommands = subCommands;
   }

   /**
    * Call {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} on a sequence of sub-commands.
    * <p>
    * Stop when one of the sub-commands throws an exception, and return an invocation stage with that exception. If all
    * the sub-commands are successful, return the {@code finalStage}. If {@code finalStage} has and exception, skip all
    * the sub-commands and just return the {@code finalStage}.
    */
   public static Object invokeEach(InvocationContext ctx, Iterator<VisitableCommand> subCommands,
                                   BaseAsyncInterceptor interceptor, Object finalReturnValue) {
      if (!subCommands.hasNext())
         return finalReturnValue;

      MultiSubCommandInvoker invoker = new MultiSubCommandInvoker(interceptor, finalReturnValue, subCommands);
      VisitableCommand newCommand = subCommands.next();
      return interceptor.invokeNextThenApply(ctx, newCommand, invoker);
   }

   @Override
   public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
      if (subCommands.hasNext()) {
         VisitableCommand newCommand = subCommands.next();
         return interceptor.invokeNext(rCtx, newCommand);
      } else {
         return finalStage;
      }
   }
}
