package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.AsyncInterceptor;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Invoke a sequence of sub-commands, allowing the caller to define a single
 * {@link AsyncInterceptor.ForkReturnHandler}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class AsyncSubCommandInvoker implements AsyncInterceptor.ForkReturnHandler {
   private Object returnValue;
   private final Iterator<VisitableCommand> subCommands;
   private final AsyncInterceptor.ForkReturnHandler finalReturnHandler;

   private AsyncSubCommandInvoker(Object returnValue, Iterator<VisitableCommand> subCommands,
         AsyncInterceptor.ForkReturnHandler finalReturnHandler) {
      this.returnValue = returnValue;
      this.subCommands = subCommands;
      this.finalReturnHandler = finalReturnHandler;
   }

   public static CompletableFuture<Void> forEach(InvocationContext ctx, VisitableCommand command,
         Object returnValue, Stream<VisitableCommand> subCommandStream,
         AsyncInterceptor.ForkReturnHandler finalReturnHandler) throws Throwable {
      AsyncSubCommandInvoker forker =
            new AsyncSubCommandInvoker(returnValue, subCommandStream.iterator(), finalReturnHandler);
      return forker.handle(ctx, command, null, null);
   }

   @Override
   public CompletableFuture<Void> handle(InvocationContext rCtx, VisitableCommand rCommand,
         Object subReturnValue, Throwable throwable) throws Throwable {
      if (throwable != null)
         throw throwable;

      if (subCommands.hasNext()) {
         VisitableCommand newCommand = subCommands.next();
         return rCtx.forkInvocation(newCommand, this);
      } else {
         return finalReturnHandler.handle(rCtx, rCommand, returnValue, null);
      }
   }
}
