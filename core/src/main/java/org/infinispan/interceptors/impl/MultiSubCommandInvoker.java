package org.infinispan.interceptors.impl;

import java.util.Iterator;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.InvocationComposeHandler;

/**
 * Invoke a sequence of sub-commands.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class MultiSubCommandInvoker implements InvocationComposeHandler {
   private final BaseAsyncInterceptor interceptor;
   private final BasicInvocationStage finalStage;
   private final Iterator<VisitableCommand> subCommands;

   private MultiSubCommandInvoker(BaseAsyncInterceptor interceptor, BasicInvocationStage finalStage,
                                  Iterator<VisitableCommand> subCommands) {
      this.interceptor = interceptor;
      this.finalStage = finalStage;
      this.subCommands = subCommands;
   }

   /**
    * Call {@link BaseAsyncInterceptor#invokeNext(InvocationContext, VisitableCommand)} on a sequence of sub-commands.
    * <p>
    * Stop when one of the sub-commands throws an exception, and return an invocation stage with that exception. If all
    * the sub-commands are successful, return the {@code finalStage}. If {@code finalStage} has and exception, skip all
    * the sub-commands and just return the {@code finalStage}.
    */
   public static BasicInvocationStage thenForEach(InvocationContext ctx, Iterator<VisitableCommand> subCommands,
                                                  BaseAsyncInterceptor interceptor, BasicInvocationStage finalStage) {
      MultiSubCommandInvoker invoker = new MultiSubCommandInvoker(interceptor, finalStage, subCommands);
      if (!subCommands.hasNext()) return finalStage;

      VisitableCommand newCommand = subCommands.next();
      return interceptor.invokeNext(ctx, newCommand).compose(invoker);
   }

   @Override
   public BasicInvocationStage apply(BasicInvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand,
                                     Object rv, Throwable t) throws Throwable {
      if (t != null) return stage;

      if (subCommands.hasNext()) {
         VisitableCommand newCommand = subCommands.next();
         return interceptor.invokeNext(rCtx, newCommand);
      } else {
         return finalStage;
      }
   }
}
