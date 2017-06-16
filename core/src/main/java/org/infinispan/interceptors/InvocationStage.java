package org.infinispan.interceptors;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;

/**
 * A partial command invocation, either completed or in progress.
 *
 * It is similar to a {@link java.util.concurrent.CompletionStage}, but it allows more callback functions
 * to be stateless by passing the context and the invoked command as parameters.
 *
 * Unlike {@link java.util.concurrent.CompletionStage}, adding a callback <em>can</em> delay the completion
 * of the initial stage and change its result.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public abstract class InvocationStage {
   /**
    * Wait for the invocation to complete and return its value.
    *
    * @throws Throwable Any exception raised during the invocation.
    */
   public abstract Object get() throws Throwable;

   /**
    * @return {@code true} if the invocation is complete.
    */
   public abstract boolean isDone();

   /**
    * {@link CompletableFuture} conversion.
    */
   public abstract CompletableFuture<Object> toCompletableFuture();

   /**
    * After the current stage completes successfully, invoke {@code function} and return its result.
    *
    * The result may be either a plain value, or a new {@link InvocationStage}.
    */
   public Object thenApply(InvocationContext ctx, VisitableCommand command,
                                    InvocationSuccessFunction function) {
      return addCallback(ctx, command, function);
   }

   public Object thenAccept(InvocationContext ctx, VisitableCommand command,
                                     InvocationSuccessAction function) {
      return addCallback(ctx, command, function);
   }

   public Object andExceptionally(InvocationContext ctx, VisitableCommand command,
                                  InvocationExceptionFunction function) {
      return addCallback(ctx, command, function);
   }

   public Object andFinally(InvocationContext ctx, VisitableCommand command,
                            InvocationFinallyAction action) {
      return addCallback(ctx, command, action);
   }

   public Object andHandle(InvocationContext ctx, VisitableCommand command,
                           InvocationFinallyFunction function) {
      return addCallback(ctx, command, function);
   }

   public abstract Object addCallback(InvocationContext ctx, VisitableCommand command,
                                      InvocationCallback function);
}
