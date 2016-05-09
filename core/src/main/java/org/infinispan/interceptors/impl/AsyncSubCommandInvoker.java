package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.SequentialInterceptor;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Invoke a sequence of sub-commands, allowing the caller to define a single
 * {@link org.infinispan.interceptors.SequentialInterceptor.ForkReturnHandler}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class AsyncSubCommandInvoker implements SequentialInterceptor.ForkReturnHandler {
   private Object returnValue;
   private final Iterator<VisitableCommand> subCommands;
   private final SequentialInterceptor.ForkReturnHandler finalReturnHandler;

   private AsyncSubCommandInvoker(Object returnValue, Iterator<VisitableCommand> subCommands,
         SequentialInterceptor.ForkReturnHandler finalReturnHandler) {
      this.returnValue = returnValue;
      this.subCommands = subCommands;
      this.finalReturnHandler = finalReturnHandler;
   }

   public static <T> CompletableFuture<Void> forEach(InvocationContext ctx, VisitableCommand command,
         Object returnValue, Stream<VisitableCommand> subCommandStream,
         SequentialInterceptor.ForkReturnHandler finalReturnHandler) throws Throwable {
      AsyncSubCommandInvoker forkInvoker =
            new AsyncSubCommandInvoker(returnValue, subCommandStream.iterator(), finalReturnHandler);
      return forkInvoker.handle(ctx, command, null, null);
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
